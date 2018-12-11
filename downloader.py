import pymysql,requests,os,time,sys,re,json

# 定义全局数据库连接
db = pymysql.connect("localhost","root","960329.ht","spider")
cursor = db.cursor()

# 基本路径列表
base_dir = [r'./tuwan/image/',r'./tuwan/music/']

# 类型列表
type_list = ['.zip','.mp3']

# 数据库列表
db_name_list = ['tuwan_image','tuwan_bgm']


def get_detial_list(id):
    url = 'https://api.tuwan.com/apps/Welfare/detail?type=image&dpr=3&id=%d&format=json'% (id)
    r = requests.get(url)
    if r.status_code == 200:
        data = json.loads(r.text)
        if data['error'] == 0:
            id = data['id']
            zip_url = data['url']
            if '.zip' in zip_url:
                title = re.sub(r'[\/:*?<>|]','',data['title'])
                sql = "insert into tuwan_image values(%d,'%s','%s','%s',0)"%(id,url,title,zip_url)
                try:
                    cursor.execute(sql)
                    db.commit()
                    print('id:%d\t图片:%s存储成功。'%(id,title))
                except:
                    db.rollback()
                    print('id:%d\t图片:%s存储失败。'%(id,title))
            if data['bgm']:
                bgm = data['bgm']
                bgm_name = re.sub(r'[\/:*?"<>|]','',data['bgm_name'])
                bgm_sql = "insert into tuwan_bgm values(%d,'%s','%s','%s',0)"%(id,url,bgm_name,bgm)
                try:
                    cursor.execute(bgm_sql)
                    db.commit()
                    print('id:%d\t音乐:%s存储成功。'%(id,bgm_name))
                except:
                    db.rollback()
                    print('id:%d\t音乐:%s存储失败。'%(id,bgm_name))


def download(data,t):
    for d in data:
        if d[4]==0:
            file_name = '%s%s%s'%((str(d[0])+'_' if t==0 else ''),d[2],type_list[t])
            file_path ='%s%s' % (base_dir[t],file_name)
            if os.path.exists(file_path):
                try:
                    sql = 'update tuwan_image set flag=%d where id=%d'%(1,d[0])
                    cursor.execute(sql)
                    db.commit()
                    print('%s 已存在，跳过'%((str(d[0])+"_" if t==0 else '')+d[2]+type_list[t]))
                except:
                    db.rollback()
                    print('更新数据库失败')
            else:
                res = requests.get(d[3],stream=True)
                total_size = int(res.headers['content-length'])
                current_size = 0
                ltime =time.localtime()
                print('在%d年%d月%d日 %d:%d:%d\t 开始下载文件: %s \t大小:%dKB'%(ltime[0],ltime[1],ltime[2],ltime[3],ltime[4],ltime[5],file_name,total_size))
                with open(file_path,'wb') as f:
                    for chunk in res.iter_content(chunk_size=100):
                        if chunk:
                            f.write(chunk)
                            f.flush()
                            current_size +=len(chunk)
                            done = int(100*current_size/total_size)
                            print('[%s%s]%d%%' %(done*'>','-'*(100-done),int(100*current_size/total_size)),end='\r',flush=True)
                try:
                    sql = 'update tuwan_image set flag=%d where id=%d'%(1,d[0])
                    cursor.execute(sql)
                    db.commit()
                    ltime = time.localtime()
                    print('\n在%d年%d月%d日 %d:%d:%d\t 文件 %s 下载完成 \t大小:%dKB\n'%(ltime[0],ltime[1],ltime[2],ltime[3],ltime[4],ltime[5],file_name,total_size))
                except:
                    db.rollback()
                    print('文件下载完成,更新数据库失败')
        else:
            print('%s 已下载，跳过'%(str(d[0])+"_"+d[2]+type_list[0]))


def get_data(table):
    sql = "select * from %s"%(table)
    try:
        cursor.execute(sql)
        data = cursor.fetchall()
        return data
    except:
        print('获取%s表的数据失败'%(table))
        return 0

# 初始化环境
def init():
    image_sql = "create table %s(id int(6) primary key,link char(255),title char(255),url char(255),flag int(1))"%db_name_list[0]
    music_sql = "create table %s(id int(6) primary key,link char(255),name char(255),url char(255),flag int(1))"%db_name_list[1]
    try:
        cursor.execute(image_sql)
        cursor.execute(music_sql)
        db.commit()
        print("数据表创建完成.")
    except Exception as e:
        db.rollback()
        print("初始化数据库失败.")
        raise e
    for d in base_dir:
        if not os.path.exists(d):
            os.makedirs(d)
            print("目录%s创建完成。"%(d))
        else:
            print("目录%s已存在，无需创建"%(d))
    
if __name__=='__main__':
    if input("初始化（数据表和目录）？\n请输入yes或no:\n") == 'yes':
        init()
    if input("开始抓取数据（已存在就无须抓取）？\n请输入yes或no:\n") =='yes':
        for i in range(0,2000):
            get_detial_list(i)
    status = input("下载数据，请选择下载音乐或图片或者退出\n0代表image 1代表music q代表退出:\n")
    if status == 'q':
        db.close()
        sys.exit("程序执行完毕")
    else:
        data = get_data(db_name_list[int(status)])
        download(data,int(status))
        db.close()
        sys.exit("程序执行完毕")
    
    
