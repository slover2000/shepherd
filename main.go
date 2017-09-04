package main

import (
	"os"
	"fmt"
	"time"
	"strings"
	"reflect"
	"net/http"

	consul "github.com/hashicorp/consul/api"
	log "github.com/Sirupsen/logrus"
	"github.com/rifflock/lfshook"
	"github.com/lestrrat/go-file-rotatelogs"
	"github.com/deckarep/golang-set"
	"github.com/koding/multiconfig"
)

const (
	// LongPollInterval the maximum duration to wait (e.g. ‘10s’) to retrieve a given index. this parameter is only applied if index is also specified. the wait time by default is 5 minutes.
	LongPollInterval = 180 * time.Second

	// SleepInterval Define a sleep time in seconds to control the frequency of watching consul server, this will protect consul server and reduce consul server load especially when internal error occurs in consul server
	SleepInterval = 3 * time.Second

	// PassingStatus passing status
	PassingStatus = "passing"

	// KeepAliveFormater Define keepalive format in UPSTREAM_FILE
	KeepAliveFormater = "    keepalive %d;"
)

type (
	// ConsulServices alias type for services
	ConsulServices	map[string][]string

	// ChangeInfo ..
	ChangeInfo struct {
		Added			[]string
		Removed 	[]string
		Changed		[]string
	}

	// NginxConfig nginx relative configs
	NginxConfig struct {
		Addr									string	`default:"http://127.0.0.1:18882"` // Define dyups management url, it's configured on the same host with nginx
		UpstreamURLPrefix			string 	`default:"http://127.0.0.1:18882/upstream/"` // Define dyups upstream api url to retrieve all upstream information
		DetailURL							string 	`default:"http://127.0.0.1:18882/detail"` // Define dyups upstream details information to retrieve server information about specific upstream
		UpstreamFilePath			string	`default:"/home/dyups/apps/config/nginx/conf.d/dyups.upstream.com.conf"` // Define upstream config file to persist servers information from consul server
		RequestTimeout				int			`default:"5"` // Define http request timeout, time unit is second
		KeepAliveConnNum 			int			`default:"20"` // Define keepalive value for synced service in UPSTREAM_FILE
		UpstreamMaxFailed			int 		`default:"3"` // Define upstream server max_fail property in UPSTREAM_FILE
		UpstreamFailedTimeout	string 	`default:"2s"` // Define upstream server fail timeout in UPSTREAM_FILE
	}

	// ServerConfig ...
	ServerConfig struct {
		LogFile					string			`default:"log.log"`
		MinServiceNum 	int 				`default:"1"` // Define a service count threshold in case of any consul server crash
		MinUpstreamNum 	int 				`default:"1"` // Define a upstream count threshold of a service
		NginxConf 			NginxConfig
	}	
)

var serverConf = new(ServerConfig)

func loadConfig(file string) {
	// read server config from file
	m := multiconfig.NewWithPath(file) // supports TOML and JSON	
	if err := m.Load(serverConf); err != nil {
		fmt.Printf("read server.toml config file failed with err:%v", err)
		os.Exit(-1)
	}
	m.MustLoad(serverConf) // Check for error	
}

func initLog(path string) {
	writer, err := rotatelogs.New(
    path + ".%Y%m%d",
    rotatelogs.WithLinkName(path),
    rotatelogs.WithMaxAge(time.Duration(7 * 24) * time.Hour),
    rotatelogs.WithRotationTime(time.Duration(24) * time.Hour),
  )

	if err != nil {
		panic("can't create rotatelogs writer")
	}	

	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.InfoLevel)
	log.SetOutput(os.Stdout)
	log.AddHook(lfshook.NewHook(lfshook.WriterMap{
		log.InfoLevel: writer,
		log.WarnLevel: writer,
		log.ErrorLevel: writer,
		log.FatalLevel: writer,
	}))
}

func getConsulServices(c *consul.Client, datacenter string, index uint64) (uint64, map[string][]string, error) {	
	catalog := c.Catalog()
	// if datacenters, err := catalog.Datacenters(); err == nil {
	// 	for _, dc := range datacenters {
	// 		fmt.Printf("dc:%s\n", dc)
	// 	}
	// }

	options := &consul.QueryOptions{Datacenter: datacenter, WaitIndex: index, WaitTime: LongPollInterval}
	serviceTags, meta, err := catalog.Services(options)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Warn("Invoke querying services failed.")
		return 0, nil, err
	}

	servierUpstream := make(ConsulServices)
	health := c.Health()
	for k := range serviceTags {
		if k != "consul" {
			if entities, _, err := health.Service(k, "", false, nil); err == nil {
				for _, entity := range entities {
					serviceAddr := fmt.Sprintf("%s:%d", entity.Node.Address, entity.Service.Port)
					checkSucc := true
					if len(entity.Checks) > 0 {
						for _, check := range entity.Checks {							
							if check.Status != PassingStatus {
								checkSucc = false
								log.WithFields(log.Fields{
									"service": entity.Service.Service,
									"id": entity.Service.ID,									
									"addr": serviceAddr,
								}).Warnf("server status is %s, it will not be updated into nginx upstream.", check.Status)
								break
							}
						}
					} else {
						checkSucc = false
						log.WithFields(log.Fields{
							"service": entity.Service.Service,
							"id": entity.Service.ID,							
							"addr": serviceAddr,
						}).Error("server does't have health check defined.")						
					}
					
					if checkSucc {
						if slice, ok := servierUpstream[entity.Service.Service]; ok {
							slice = append(slice, serviceAddr)
							servierUpstream[entity.Service.Service] = slice
						} else {
							servierUpstream[entity.Service.Service] = []string{serviceAddr}
						}
					}
				}
			}
		}
	}	

	return meta.LastIndex, servierUpstream, nil
}

func convertToStringSlice(set mapset.Set) []string {
	values := set.ToSlice()
	slices := make([]string, len(values))
	for i, v := range values {
		str := v.(string)
		slices[i] = str
	}

	return slices
}

func getChanges(old, current ConsulServices) *ChangeInfo {
	oldSet := mapset.NewSet()
	for k := range old {
		oldSet.Add(k)
	}

	currentSet := mapset.NewSet()
	for k := range current {
		currentSet.Add(k)
	}

	intersect := currentSet.Intersect(oldSet)
	added := currentSet.Difference(intersect)
	removed := oldSet.Difference(intersect)
	changed := mapset.NewSet()
	for _, k := range intersect.ToSlice() {
		key := k.(string)
		oldValue := old[key]
		curValue := current[key]
		if !reflect.DeepEqual(oldValue, curValue) {
			changed.Add(key)
		}
	}

	return &ChangeInfo{
		Added: convertToStringSlice(added), 
		Removed: convertToStringSlice(removed), 
		Changed: convertToStringSlice(changed)}
}

// Persist consul service changes into nginx upstream configuration files in case of losing data when reloading.
func persistUpstream(current ConsulServices) {
	log.WithFields(log.Fields{
		"services": current,
	}).Info("Persist consul serivce changes into nginx's upstream configs.")

	file, err := os.Create(serverConf.NginxConf.UpstreamFilePath)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("Persist nginx's upstream config file failed.")
		return
	}
	defer file.Close()

	for name, values := range current {
		if len(values) > 0 {
			upstream := fmt.Sprintf("upstream %s  {\n", name)
			file.WriteString(upstream)
			for _, server := range values {
				upstreamServer := fmt.Sprintf(
					"    server %s max_fails=%d fail_timeout=%s;", 
					server, 
					serverConf.NginxConf.UpstreamMaxFailed, 
					serverConf.NginxConf.UpstreamFailedTimeout)
				file.WriteString(upstreamServer + "\n")
			}
			keepAlive := fmt.Sprintf(KeepAliveFormater, serverConf.NginxConf.KeepAliveConnNum)
			file.WriteString(keepAlive + "\n")
			file.WriteString("}" + "\n")
			file.WriteString("\n")
		}
	}
	log.Info("Persist configs finished.")
}

// add a service upstream servers to nginx
func addNginxUpstreamServer(serviceName, upstreamServers string) {
	addURL := fmt.Sprintf("%s%s", serverConf.NginxConf.UpstreamURLPrefix, serviceName)
	resp, err := http.Post(addURL, "text/plain", strings.NewReader(upstreamServers))
	if err != nil {
		log.WithFields(log.Fields{
			"service": serviceName,
			"servers": upstreamServers,
			"error": err,
		}).Error("Add upstream servers to nginx failed.")
		return
	}

	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		log.WithFields(log.Fields{
			"service": serviceName,
			"servers": upstreamServers,
		}).Info("Add upstream servers to nginx success.")
	}
}

// delete a service from nginx upstreams
func delNginxUpstream(serviceName string) {
	delURL := fmt.Sprintf("%s%s", serverConf.NginxConf.UpstreamURLPrefix, serviceName)
	client := http.Client{Timeout: time.Duration(serverConf.NginxConf.RequestTimeout) * time.Second}
	request, _ := http.NewRequest("DELETE", delURL, nil)
	resp, err := client.Do(request)
	if err != nil {
		log.WithFields(log.Fields{
			"service": serviceName,
			"error": err,
		}).Error("Delete service from nginx failed.")
		return
	}

	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		log.WithFields(log.Fields{
			"service": serviceName,
		}).Info("Delete service from nginx success.")
	}
}

func updateNginxUpstream(old, current ConsulServices) {
	changeInfo := getChanges(old, current)
	if len(changeInfo.Added) > 0 {
		for _, name := range changeInfo.Added {
			addedServers := current[name]
			upstreamServers := ""
			for _, server := range addedServers {
				upstreamServer := fmt.Sprintf("server %s;", server)
				upstreamServers = upstreamServers + upstreamServer
			}
			log.Info(fmt.Sprintf("add new service %s upstream servers", name))
			addNginxUpstreamServer(name, upstreamServers)
		}
	}

	if len(changeInfo.Changed) > 0 {
		for _, name := range changeInfo.Changed {
			changedServers := current[name]
			if len(changedServers) < serverConf.MinUpstreamNum {
				oldServers := old[name]
				log.WithFields(log.Fields{
					"service": name,
					"oldservers": oldServers,
					"newservers": changedServers,
					"threhold": serverConf.MinServiceNum,
				}).Warn("changed upstream servers less than threshold, so ignore it.")
				continue
			}

			upstreamServers := ""
			for _, server := range changedServers {
				upstreamServer := fmt.Sprintf("server %s;", server)
				upstreamServers = upstreamServers + upstreamServer
			}
			log.Info(fmt.Sprintf("service %s upstream servers has changed.", name))
			addNginxUpstreamServer(name, upstreamServers)
		}
	}

	if len(changeInfo.Removed) > 0 {
		for _, name := range changeInfo.Removed {			
			log.Info(fmt.Sprintf("Begin to remove upstream %s it's not in consul now!", name))
			delNginxUpstream(name)
		}
	}

	if len(changeInfo.Added) > 0 || len(changeInfo.Removed) > 0 || len(changeInfo.Changed) > 0 {
		persistUpstream(current)
	}	
}

func main() {
	// Get a new client
	client, err := consul.NewClient(consul.DefaultConfig())
	if err != nil {
			panic(err)
	}

	loadConfig("./config.toml")
	initLog(serverConf.LogFile)

	lastIndex := uint64(0)
	oldServices := make(ConsulServices)
	dc := ""
	for {
		if newIndex, services, err := getConsulServices(client, dc, lastIndex); err == nil {
			if len(services) < serverConf.MinServiceNum {
				log.Warnf("Consul service number is less than %d , please check the system!", serverConf.MinServiceNum)
				continue
			}

			if newIndex != lastIndex {
				log.WithFields(log.Fields{
					"old_index": lastIndex,
					"new_index": newIndex,
				}).Warn("Querying index had changed.")
				lastIndex = newIndex
	
				updateNginxUpstream(oldServices, services)
			}
		}

		time.Sleep(SleepInterval)
	}
}