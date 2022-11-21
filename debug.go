package geerpc

import (
	"fmt"
	"html/template"
	"net/http"
)

const debugText = `<html>
	<body>
	<title>GeeRPC Services</title>
	{{range .}}
	<hr>
	Service {{.Name}}
	<hr>
		<table>
		<th align=center>Method</th><th align=center>Calls</th>
		{{range $name, $methodType := .Methods}}
			<tr>
			<td align=left font=fixed>{{$name}}({{$methodType.ArgType}}, {{$methodType.ReplyType}}) error</td>
			<td align=center>{{$methodType.NumCalls}}</td>
			</tr>
		{{end}}
		</table>
	{{end}}
	</body>
	</html>`

var debug = template.Must(template.New("RPC debug").Parse(debugText))

type debugHTTP struct {
	*Server
}

type debugService struct {
	Name    string
	Methods map[string]*methodType
}

// ServeHTTP Runs at /debug/geerpc
func (server debugHTTP) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// Build a sorted version of the data
	var services []debugService
	server.serviceMap.Range(func(key, value any) bool {
		svc := value.(*service)
		services = append(services, debugService{Name: key.(string), Methods: svc.methods})
		return true
	})
	err := debug.Execute(w, services)
	if err != nil {
		_, _ = fmt.Fprintln(w, "rpc: error executing template: ", err.Error())
	}
}
