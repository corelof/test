package proto

type UpdateLocationRequest struct{}

type GetDistanceRequest struct {
	PointX int
	PointY int
}

type GetDistanceResponse struct {
	Distance *float64
}
