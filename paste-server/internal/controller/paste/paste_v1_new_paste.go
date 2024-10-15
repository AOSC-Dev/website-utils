package paste

import (
	"context"
	"fmt"

	"github.com/gogf/gf/v2/encoding/gjson"
	"github.com/gogf/gf/v2/errors/gcode"
	"github.com/gogf/gf/v2/errors/gerror"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gfile"
	"github.com/gogf/gf/v2/os/gtime"
	"github.com/google/uuid"

	v1 "pasteServer/api/paste/v1"
	"pasteServer/internal/consts"
)

func (c *ControllerV1) NewPaste(ctx context.Context, req *v1.NewPasteReq) (res *v1.NewPasteRes, err error) {
	pasteId := uuid.New().String()
	res = &v1.NewPasteRes{Id: pasteId}

	fmt.Println(req.ExpDate)

	// 如果过期时间为空,默认生成7天后过期
	if req.ExpDate == "" {
		now := gtime.Now()
		now = now.AddDate(0, 0, 7)
		req.ExpDate = now.String()[:10]
	}
	expDate, _ := gtime.StrToTime(req.ExpDate + " 00:00:00")

	gfile.Mkdir(consts.PasteContentPath + pasteId)
	pasteContent := v1.PasteContent{
		Content:   req.Content,
		Language:  req.Language,
		Title:     req.Title,
		ExpDate:   req.ExpDate,
		ExpDateTs: expDate.Timestamp(),
	}

	if req.FileList != nil {
		filenames, e := req.FileList.Save(consts.PasteContentPath+pasteId+"/files/", false)
		if e != nil {
			err = gerror.NewCode(gcode.CodeInternalError, "保存附件文件失败")
			g.Log("保存附件文件失败: ", e.Error())
			return
		}
		pasteContent.FileList = filenames
	} else {
		pasteContent.FileList = make([]string, 0)
	}

	json := gjson.New(pasteContent)
	jsonStr, e := json.ToJsonString()
	if e != nil {
		err = gerror.NewCode(gcode.CodeInternalError, "保存json文件失败")
		g.Log("保存json文件失败: ", e.Error())
		return
	}
	gfile.PutContents(consts.PasteContentPath+pasteId+"/content.json", jsonStr)

	// 将删除文件放到删除目录
	gfile.PutContents(consts.PasteRemovePath+req.ExpDate+"/"+pasteId, "")

	return
}
