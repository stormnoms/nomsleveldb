// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package chunks

import (
	//"fmt"
	"github.com/attic-labs/testify/suite"

	"github.com/stormasm/nomsleveldb/go/constants"
	"github.com/stormasm/nomsleveldb/go/hash"
)

type ChunkStore1TestSuite struct {
	suite.Suite
	Store      ChunkStore
	putCountFn func() int
}

func (suite *ChunkStore1TestSuite) TestChunkStorePut() {
	hash_str_check1 := "rmnjb8cjc5tblj21ed4qs821649eduie"
	input1 := "abc"
	c1 := NewChunk([]byte(input1))
	suite.Store.Put(c1)
	h1 := c1.Hash()

	// See http://www.di-mgt.com.au/sha_testvectors.html
	suite.Equal(hash_str_check1, h1.String())

	oldRoot := suite.Store.Root()
	suite.True(oldRoot.IsEmpty())

	suite.Store.UpdateRoot(h1, suite.Store.Root()) // Commit writes

	myhash1 := suite.Store.Root()
	myhash_str1 := myhash1.String()
	//fmt.Println(myhash_str1)
	suite.Equal(hash_str_check1, myhash_str1)

	// And reading it via the API should work...
	assertInputInStore(input1, h1, suite.Store, suite.Assert())
	if suite.putCountFn != nil {
		suite.Equal(1, suite.putCountFn())
	}

	// Re-writing the same data should cause a second put
	c1 = NewChunk([]byte(input1))
	suite.Store.Put(c1)
	suite.Equal(h1, c1.Hash())
	assertInputInStore(input1, h1, suite.Store, suite.Assert())
	suite.Store.UpdateRoot(h1, suite.Store.Root()) // Commit writes

	if suite.putCountFn != nil {
		suite.Equal(2, suite.putCountFn())
	}

	//
	// Add a second input
	//

	hash_str_check2 := "82k5bfoaif0g37blrldljjc1atg8g4et"
	input2 := "def"
	c2 := NewChunk([]byte(input2))
	suite.Store.Put(c2)
	h2 := c2.Hash()

	//fmt.Println(h2.String())
	suite.Equal(hash_str_check2, h2.String())

	oldRoot = suite.Store.Root()
	//fmt.Println(oldRoot)
	suite.Equal(oldRoot,h1)

	suite.Store.UpdateRoot(h2, suite.Store.Root()) // Commit writes

	myhash2 := suite.Store.Root()
	myhash_str2 := myhash2.String()
	//fmt.Println(myhash_str2)

	suite.Equal(hash_str_check2, myhash_str2)

	// And reading it via the API should work...
	assertInputInStore(input2, h2, suite.Store, suite.Assert())
	if suite.putCountFn != nil {
		suite.Equal(3, suite.putCountFn())
	}
}

func (suite *ChunkStore1TestSuite) TestChunkStoreRoot() {
	oldRoot := suite.Store.Root()
	suite.True(oldRoot.IsEmpty())

	bogusRoot := hash.Parse("8habda5skfek1265pc5d5l1orptn5dr0")
	newRoot := hash.Parse("8la6qjbh81v85r6q67lqbfrkmpds14lg")

	// Try to update root with bogus oldRoot
	result := suite.Store.UpdateRoot(newRoot, bogusRoot)
	suite.False(result)

	// Now do a valid root update
	result = suite.Store.UpdateRoot(newRoot, oldRoot)
	suite.True(result)
}

func (suite *ChunkStore1TestSuite) TestChunkStoreVersion() {
	oldRoot := suite.Store.Root()
	suite.True(oldRoot.IsEmpty())
	newRoot := hash.Parse("11111222223333344444555556666677")
	suite.True(suite.Store.UpdateRoot(newRoot, oldRoot))

	suite.Equal(constants.NomsVersion, suite.Store.Version())
}
