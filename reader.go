package clickhouse

import (
	"io"

	"github.com/chzyer/readline"
)

// Reader 命令行读取器
type Reader struct {
	rl *readline.Instance
}

// NewReader 创建新的读取器
func NewReader(term Terminal) *Reader {
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "",
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
		Stdin:           term,
		Stdout:          term,
	})
	if err != nil {
		panic(err)
	}
	return &Reader{rl: rl}
}

// ReadLine 读取一行输入
func (r *Reader) ReadLine() (string, error) {
	line, err := r.rl.Readline()
	if err == readline.ErrInterrupt {
		return "", nil
	} else if err == io.EOF {
		return "exit", nil
	}
	return line, err
}

// Close 关闭读取器
func (r *Reader) Close() error {
	return r.rl.Close()
}

