package influx_writer


import (
	cl "github.com/influxdata/influxdb/client"
	"net/url"
	"github.com/antigloss/go/logger"
	"errors"
	"fmt"
)


var err error
var defaultPrecision string
var influxConnected bool

type InfluxWriter struct{
	InfluxCfg *InfluxConfig
	InfluxClient *cl.Client
}

type InfluxConfig struct{
	Enabled bool
	Server string
	Precision string
	DatabaseName string
	Measurement string
	IsConnected bool
}

func (ic *InfluxConfig) CreateInfluxWriter() (InfWr InfluxWriter, err error){
	InfWr.InfluxCfg = ic
	icl, err := InfWr.InfluxCfg.GetInfluxClient()
	if err != nil{
			return
	}
	InfWr.InfluxClient = icl
	InfWr.InfluxCfg.IsConnected = true
	return
}

func SetInfluxConfig(enable bool,server string,precision string, dbname string, measurement string) (InfConf InfluxConfig){
	return InfluxConfig{enable,server,precision,dbname,measurement, false}
}

func (ic *InfluxConfig) GetInfluxClient() (InfClient *cl.Client, err error){
	u, err := url.Parse(ic.Server)
	if err != nil {

		logger.Error("Error parsing URL, error : ", err)
		return nil,err
	}

	InfClient, err = cl.NewClient( cl.Config{
		URL: *u,
		Precision: ic.Precision,
	})
	return
}

func (iw *InfluxWriter) WriteRow(message string) (err error){
	if iw.InfluxCfg.Enabled == false || iw.InfluxCfg.IsConnected == false {
		err = errors.New("Influxdb not enabled or connected")
		return
	}

	if len(message) == 0 {
		err = errors.New("Got blank query on influxdb")
		logger.Error(err.Error())
		return
	}


	var bp cl.BatchPoints
	var pts []cl.Point

	pts = append(pts, cl.Point{	Raw: message})

	bp.Database = iw.InfluxCfg.DatabaseName
	bp.Points = pts

	_, err = iw.InfluxClient.Write(bp)

	if err != nil {
		err = errors.New(fmt.Sprintf("Could not insert data to influx, got error : %v", err))
		logger.Error(err.Error())
	}

	return
}

func (iw *InfluxWriter) WriteRows(messages []string, InfClient cl.Client) (err error){
	for _,valx := range messages{
		err = iw.WriteRow(valx)
		if err != nil{
			return
		}
	}
	return nil
}
