/*
Copyright 2018 The vegamcache Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vegamcache

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/weaveworks/mesh"
)

type Value struct {
	Data      interface{}
	LastWrite int64
	Expiry    int64
}
type cache struct {
	set sync.Map
}

var _ mesh.GossipData = &cache{}

type externalCache struct {
	cache *cache
}

func NewCache() *externalCache {
	return &externalCache{&cache{}}
}

func (ec *externalCache) Get(key string) (interface{}, bool) {
	return ec.cache.get(key)
}

func (ec *externalCache) Put(key string, val interface{}, ttl time.Duration) {
	var expiryTime int64
	if ttl == 0 {
		expiryTime = 0
	} else {
		expiryTime = time.Now().Add(ttl).UnixNano()
	}
	ec.cache.put(key, Value{
		Data:   val,
		Expiry: expiryTime,
	})
}
func (c *cache) Encode() [][]byte {
	tmpMap := make(map[string]Value)
	c.set.Range(func(k, v interface{}) bool {
		tmpMap[k.(string)] = v.(Value)
		return true
	})
	buf, err := json.Marshal(tmpMap)
	if err != nil {
		panic(err)
	}
	return [][]byte{buf}
}

func (c *cache) Merge(other mesh.GossipData) mesh.GossipData {
     tmpMap := make(map[string]Value)
	other.(*cache).set.Range(func(k, v interface{}) bool {
		tmpMap[k.(string)] = v.(Value)
		return true
	})
	return c.mergeComplete(tmpMap)
}

func (c *cache) mergeComplete(other map[string]Value) mesh.GossipData {
	for k, v := range other {
		val, ok := c.set.Load(k)
		if !ok {
			if v.Expiry < time.Now().UnixNano() {
				continue
			}
			c.set.Store(k, v)
			continue
		}
		// checking existing expiry
		if val.(Value).Expiry < time.Now().UnixNano() {
			c.set.Delete(k)
		}
		if val.(Value).LastWrite < v.LastWrite {
		     c.set.Store(k, v)
			continue
		}
	}
	return c
}

func (c *cache) mergeDelta(set map[string]Value) (delta mesh.GossipData) {
	for k, v := range set {
		val, ok := c.set.Load(k)
		if ok && val.(Value).LastWrite > v.LastWrite {
			c.set.Delete(k)
			continue
		}
		// expired value is not added
		if val.(Value).Expiry != 0 && val.(Value).Expiry < time.Now().UnixNano() {
			c.set.Delete(k)
			continue
		}
		c.set.Store(k, v)
	}
	
	// TODO check
	//return &cache{set: set}
	return c
}

func (c *cache) mergeRecived(set map[string]Value) (recived mesh.GossipData) {
	for k, v := range set {
		val, ok := c.set.Load(k)
		if ok && val.(Value).LastWrite > v.LastWrite {
			c.set.Delete(k)
			continue
		}
		// expired value is not added
		if val.(Value).Expiry != 0 && val.(Value).Expiry < time.Now().UnixNano() {
			c.set.Delete(k)
			continue
		}
		c.set.Store(k, v)
	}
	if len(set) == 0 {
		return nil
	}
	
	// TODO check
	//return &cache{set: set}
	return c
}

/*func (c *cache) copy() *cache {
	return &cache{
		set: c.set,
	}
}*/

func (c *cache) get(key string) (interface{}, bool) {
	if val, ok := c.set.Load(key); ok {
		if val.(Value).Expiry == 0 || val.(Value).Expiry > time.Now().UnixNano() {
			return val.(Value).Data, true
		}
		c.set.Delete(key)
		return nil, false
	}
	return nil, false
}

func (c *cache) put(key string, val Value) {
	c.set.Store(key, val)
}
