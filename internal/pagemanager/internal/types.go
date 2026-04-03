package internal

import (
	api "github.com/akzj/go-fast-kv/internal/pagemanager/api"
)

// Use PageID from the API package to ensure interface compatibility.
type PageID = api.PageID

const PageIDInvalid = api.PageIDInvalid
