package sqlite3

import "testing"

func TestSqlite3Storage_Visited(t *testing.T) {
	s := &Sqlite3Storage{
		Filename: "./visited_test.db",
	}

	if err := s.Init(); err != nil {
		t.Error("failed to initialize storage backend: " + err.Error())
		return
	}

	//defer s.Clear()

	ids := []uint64{1,2,3,4,5}

	for _, u := range ids{
		if err := s.Visited(u); err != nil {
			t.Error("failed to add visit: " + err.Error())
		}
	}

	for _, u := range ids{
		if _, err := s.IsVisited(u); err != nil {
			t.Error("failed to check visit: " + err.Error())
		}
	}

}

func TestSqlite3Storage_AddRequest(t *testing.T) {
	s := &Sqlite3Storage{
		Filename: "./queue_add_test.db",
	}

	if err := s.Init(); err != nil {
		t.Error("failed to initialize storage backend: " + err.Error())
		return
	}

	//defer s.Clear()

	request1 := []byte{1,2,3,4,5,6}
	request2 := []byte{7,8,9,10,11,12}

	if err := s.AddRequest(request1); err !=nil {
		t.Error("failed to AddRequest" + err.Error())
	}

	if err := s.AddRequest(request2); err !=nil {
		t.Error("failed to AddRequest" + err.Error())
	}

}

func TestSqlite3Storage_QueueSize(t *testing.T) {
	s := &Sqlite3Storage{
		Filename: "./queue_size_test.db",
	}

	if err := s.Init(); err != nil {
		t.Error("failed to initialize storage backend: " + err.Error())
		return
	}

	//defer s.Clear()

	request1 := []byte{1,2,3,4,5,6}
	request2 := []byte{7,8,9,10,11,12}

	if err := s.AddRequest(request1); err !=nil {
		t.Error("failed to AddRequest" + err.Error())
	}

	if err := s.AddRequest(request2); err !=nil {
		t.Error("failed to AddRequest" + err.Error())
	}

	if size, err := s.QueueSize(); err !=nil {
		t.Error("failed to get queue size: " + err.Error())
		return
	} else if size !=2 {
		t.Errorf("queue size is not correct should be 2 but is: %v", size)
	} else {
		t.Log("queue size is correct")
	}
}

func TestSqlite3Storage_GetRequest(t *testing.T) {
	count := 0

	s := &Sqlite3Storage{
		Filename: "./queue_get_test.db",
	}

	if err := s.Init(); err != nil {
		t.Error("failed to initialize storage backend: " + err.Error())
		return
	}

	//defer s.Clear()

	request1 := []byte{1,2,3,4,5,6}
	request2 := []byte{7,8,9,10,11,12}

	if err := s.AddRequest(request1); err !=nil {
		t.Error("failed to AddRequest" + err.Error())
	}

	if err := s.AddRequest(request2); err !=nil {
		t.Error("failed to AddRequest" + err.Error())
	}

	for {
		b, err := s.GetRequest()
		if b == nil {break}
		t.Logf("b: %v", b)
		if err != nil {
			t.Error("failed to GetRequest: " + err.Error())
			break
		}

		count += 1

		if count > 2{
			t.Error("failed to get 2 requests; got more")
			break
		}
	}
}