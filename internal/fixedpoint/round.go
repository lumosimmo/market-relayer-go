package fixedpoint

import "math/big"

func MustRoundRatioHalfAway(numerator, denominator int64) int64 {
	if denominator <= 0 {
		panic("fixedpoint: denominator must be positive")
	}
	return MustDivRoundHalfAwayFromZero(big.NewInt(numerator), big.NewInt(denominator)).Int64()
}

func MustDivRoundHalfAwayFromZero(numerator, divisor *big.Int) *big.Int {
	return divRoundHalfAwayFromZero(numerator, divisor)
}

func divRoundHalfAwayFromZero(numerator, divisor *big.Int) *big.Int {
	if divisor.Sign() <= 0 {
		panic("fixedpoint: divisor must be positive")
	}

	negative := numerator.Sign() < 0
	absNumerator := new(big.Int).Abs(numerator)

	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(absNumerator, divisor, remainder)

	doubleRemainder := new(big.Int).Mul(remainder, twoBig)
	if doubleRemainder.Cmp(divisor) >= 0 {
		quotient.Add(quotient, oneBig)
	}

	if negative {
		quotient.Neg(quotient)
	}

	return quotient
}
