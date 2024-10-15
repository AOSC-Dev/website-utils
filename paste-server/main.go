package main

import (
	"context"
	"fmt"
	"pasteServer/internal/cmd"
	"pasteServer/internal/consts"
	_ "pasteServer/internal/packed"

	"github.com/gogf/gf/v2/os/gcron"
	"github.com/gogf/gf/v2/os/gctx"
	"github.com/gogf/gf/v2/os/gfile"
	"github.com/gogf/gf/v2/os/gtime"
)

func main() {
	ctx := gctx.GetInitCtx()
	// 使用cron定时器,每天执行一次,删除过期粘贴板
	gcron.Add(ctx, "* * 0 * * *", func(ctx context.Context) {
		fmt.Println("clear paste of expired")
		nowStr := gtime.Now().String()[:10]

		// 查找比今天早的时间
		dirList, _ := gfile.DirNames(consts.PasteRemovePath)
		for _, dir := range dirList {
			if dir < nowStr {
				fileList, _ := gfile.DirNames(consts.PasteRemovePath + dir)
				for _, f := range fileList {
					gfile.Remove(consts.PasteContentPath + f)
				}
				gfile.Remove(consts.PasteRemovePath + dir)
			}
		}
	}, "clear paste of expired")

	cmd.Main.Run(ctx)

}
