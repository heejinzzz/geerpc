package codec

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
)

type JsonCodec struct {
	conn io.ReadWriteCloser
	buf  *bufio.Writer // use a buffered writer to prevent blocking and improve performance
	enc  *json.Encoder
	dec  *json.Decoder
}

func NewJsonCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &JsonCodec{
		conn: conn,
		buf:  buf,
		enc:  json.NewEncoder(buf),
		dec:  json.NewDecoder(conn),
	}
}

func (c *JsonCodec) ReadHeader(h *Header) error {
	return c.dec.Decode(h)
}

func (c *JsonCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body)
}

func (c *JsonCodec) Write(h *Header, body interface{}) error {
	defer func() {
		err := c.buf.Flush()
		if err != nil {
			log.Println("rpc buffer write error: ", err)
			panic(err)
		}
	}()
	if err := c.enc.Encode(h); err != nil {
		log.Println("rpc codec: json encoding header error: ", err)
		return err
	}
	if err := c.enc.Encode(body); err != nil {
		log.Println("rpc codec: json encoding body error: ", err)
		return err
	}
	return nil
}

func (c *JsonCodec) Close() error {
	return c.conn.Close()
}
