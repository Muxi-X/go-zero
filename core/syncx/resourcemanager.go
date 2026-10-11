package syncx

import (
	"io"
	"sync"

	"github.com/zeromicro/go-zero/core/errorx"
)

// A ResourceManager is a manager that used to manage resources.
type ResourceManager struct {
	resources    map[string]io.Closer
	singleFlight SingleFlight
	lock         sync.RWMutex
}

// NewResourceManager returns a ResourceManager.
func NewResourceManager() *ResourceManager {
	return &ResourceManager{
		resources:    make(map[string]io.Closer),
		singleFlight: NewSingleFlight(),
	}
}

// Close closes the manager.
// Don't use the ResourceManager after Close() called.
func (manager *ResourceManager) Close() error {
	manager.lock.Lock()
	defer manager.lock.Unlock()

	var be errorx.BatchError
	for _, resource := range manager.resources {
		if err := resource.Close(); err != nil {
			be.Add(err)
		}
	}

	// release resources to avoid using it later
	manager.resources = nil

	return be.Err()
}

// GetResource returns the resource associated with given key.
func (manager *ResourceManager) GetResource(key string, create func() (io.Closer, error)) (io.Closer, error) {
	val, err := manager.singleFlight.Do(key, func() (interface{}, error) {
		manager.lock.RLock()
		resource, ok := manager.resources[key]
		manager.lock.RUnlock()
		if ok {
			return resource, nil
		}

		resource, err := create()
		if err != nil {
			return nil, err
		}

		manager.lock.Lock()
		defer manager.lock.Unlock()
		manager.resources[key] = resource

		return resource, nil
	})
	if err != nil {
		return nil, err
	}

	return val.(io.Closer), nil
}

// Inject injects the resource associated with given key.
func (manager *ResourceManager) Inject(key string, resource io.Closer) {
	manager.lock.Lock()
	manager.resources[key] = resource
	manager.lock.Unlock()
}

// RemoveResourceIf removes the resource associated with given key, but only
// when it is still the given resource. If the key holds a different resource
// (e.g. one recreated concurrently), it is left untouched and nil is returned.
// The resource is matched by identity, so its dynamic type must be comparable.
func (manager *ResourceManager) RemoveResourceIf(key string, resource io.Closer) error {
	manager.lock.Lock()
	current, ok := manager.resources[key]
	if ok && current == resource {
		delete(manager.resources, key)
	} else {
		ok = false
	}
	manager.lock.Unlock()

	if !ok {
		return nil
	}

	return resource.Close()
}
