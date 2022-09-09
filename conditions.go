package statistica

type Condition string

const (
	CondEq  Condition = "eq"
	CondEq2 Condition = "="

	CondNotEq  Condition = "neq"
	CondNotEq2 Condition = "!="

	CondLike Condition = "like"

	CondGreater     Condition = ">"
	CondGreaterOrEq Condition = ">="
	CondLess        Condition = "<"
	CondLessOrEq    Condition = "<="
)
