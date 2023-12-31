package lsmt

import (
	"LSM-Tree/core"
	"reflect"
	"testing"
)

func TestDiskFileConstruction(t *testing.T) {
	elems := []*core.Element{
		{Key: "1", Value: "One"},
		{Key: "2", Value: "Two"},
		{Key: "3", Value: "Three"},
		{Key: "4", Value: "Four"},
		{Key: "5", Value: "Five"},
		{Key: "6", Value: "Six"},
		{Key: "7", Value: "Seven"},
	}
	d := NewDiskFile(elems, 0)
	got := d.AllElements()
	// for _, e := range got {
	// 	fmt.Printf("%v", e)
	// }
	if !reflect.DeepEqual(elems, got) {
		t.Errorf("all elements got %v; want %v", got, elems)
	}

	// 再测一次
	got = d.AllElements()
	if !reflect.DeepEqual(elems, got) {
		t.Errorf("all elements got %v; want %v", got, elems)
	}
}

func TestDiskFileSearch(t *testing.T) {
	elems := []*core.Element{
		{Key: "1", Value: "One"},
		{Key: "2", Value: "Two"},
		{Key: "3", Value: "Three"},
		{Key: "4", Value: "Four"},
		{Key: "5", Value: "Five"},
		{Key: "6", Value: "Six"},
		{Key: "7", Value: "Seven"},
	}
	d := NewDiskFile(elems, 0)
	for _, e := range elems {
		if got, err := d.Search(e.Key); err != nil || got.Key != e.Key {
			t.Errorf("search got key %s, %v; want %s, nil", got.Key, err, e.Key)
		}
	}
	if got, err := d.Search("0"); err == nil {
		t.Errorf("search 0 got key %s; want not found", got.Key)
	}
	if got, err := d.Search("8"); err == nil {
		t.Errorf("search 8 got key %s; want not found", got.Key)
	}
	if got, err := d.Search("3.5"); err == nil {
		t.Errorf("search 3.5 got key %s; want not found", got.Key)
	}
}
