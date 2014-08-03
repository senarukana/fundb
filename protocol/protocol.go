package protocol

func (self *FieldValue) GetValue() interface{} {
	if self.StrVal != nil {
		return *self.StrVal
	}

	if self.DoubleVal != nil {
		return *self.DoubleVal
	}

	if self.IntVal != nil {
		return *self.IntVal
	}

	if self.BoolVal != nil {
		return *self.BoolVal
	}
	return nil
}

func (self *Record) GetFieldValue(idx int) interface{} {
	return self.Values[idx].GetValue()
}
