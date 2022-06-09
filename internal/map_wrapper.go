package internal

type MapWrapper struct {
	m map[string]string
}

func (c *MapWrapper) Find(key string) (string, error) {
	value, ok := c.m[key]
	if !ok {
		return "", KeyNotFound
	}

	return value, nil
}
