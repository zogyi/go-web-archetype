package util

import (
	"github.com/dgrijalva/jwt-go"
	"time"
)

var (
	ConstantAppName         = `Hotel Management`
	ConstantVersion         = `0.0.1`
	ConstantAccessTokenKey  = []byte("SGAU892T3JkhgJKSY8")
	ConstantRefreshTokenKey = []byte("dajgd821GS8AT2JKSDGK")
	AccessTokenExpireIn     = 30 * time.Minute
	RefreshTokenExpireIn    = 7 * 24 * time.Hour
)

type Claims struct {
	UserID     uint64 `json:"user_id"`
	UserName   string `json:"username"`
	DeviceID   string `json:"device_id"`
	AppVersion string `json:"app_version"`
	AppName    string `json:"app_name"`
	jwt.StandardClaims
}

type RefreshClaims struct {
	UserID     uint64 `json:"user_id"`
	AppVersion string `json:"app_version"`
	AppName    string `json:"app_name"`
	jwt.StandardClaims
}

type SignPairEntity struct {
	AccessToken        string    `json:"accessToken"`
	AccessTokenExpire  time.Time `json:"accessTokenExpire"`
	RefreshToken       string    `json:"refreshToken"`
	RefreshTokenExpire time.Time `json:"refreshTokenExpire"`
}

func SignAccessToken(uid uint64, username string) (string, time.Time, error) {
	expirationTime := time.Now().Add(AccessTokenExpireIn)
	claims := &Claims{
		UserID:     uid,
		UserName:   username,
		AppName:    ConstantAppName,
		AppVersion: ConstantVersion,
		StandardClaims: jwt.StandardClaims{
			ExpiresAt: expirationTime.Unix(),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenStr, err := token.SignedString(ConstantAccessTokenKey)
	if err != nil {
		return ``, expirationTime, err
	}
	return tokenStr, expirationTime, err
}

func SignRefreshToken(uid uint64) (string, time.Time, error) {
	expirationTime := time.Now().Add(24 * time.Hour * 7)
	refreshClaims := &RefreshClaims{
		UserID:     uid,
		AppVersion: ConstantVersion,
		AppName:    ConstantAppName,
		StandardClaims: jwt.StandardClaims{
			ExpiresAt: expirationTime.Unix(),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, refreshClaims)
	tokenStr, err := token.SignedString(ConstantRefreshTokenKey)
	if err != nil {
		return ``, expirationTime, err
	}
	return tokenStr, expirationTime, nil
}

func SignPair(uid uint64, username string) (SignPairEntity, error) {
	pair := SignPairEntity{}
	accessToken, accessTokenExpire, err := SignAccessToken(uid, username)
	if err != nil {
		return pair, err
	}
	pair.AccessToken = accessToken
	pair.AccessTokenExpire = accessTokenExpire
	refreshToken, refreshTokenExpire, err := SignRefreshToken(uid)
	if err != nil {
		return pair, err
	}
	pair.RefreshToken = refreshToken
	pair.RefreshTokenExpire = refreshTokenExpire
	return pair, nil
}
