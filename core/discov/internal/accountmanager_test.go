package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zeromicro/go-zero/core/stringx"
)

func TestAccount(t *testing.T) {
	// The accounts are stored globally, so the endpoints have to be unique for
	// repeated runs (-count=N).
	suffix := stringx.Rand()
	endpoints := []string{
		"account-2-" + suffix + ":2379",
		"account-3-" + suffix + ":2379",
		"account-4-" + suffix + ":2379",
	}
	username := "foo" + stringx.Rand()
	password := "bar"
	anotherPassword := "any"

	_, ok := GetAccount(endpoints)
	assert.False(t, ok)

	AddAccount(endpoints, username, password)
	account, ok := GetAccount(endpoints)
	assert.True(t, ok)
	assert.Equal(t, username, account.User)
	assert.Equal(t, password, account.Pass)

	AddAccount(endpoints, username, anotherPassword)
	account, ok = GetAccount(endpoints)
	assert.True(t, ok)
	assert.Equal(t, username, account.User)
	assert.Equal(t, anotherPassword, account.Pass)
}
