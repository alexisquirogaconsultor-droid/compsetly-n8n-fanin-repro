"""Finite native n8n experiment using synthetic data and a loopback HTTP counter."""
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import argparse
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import threading

ROOT=Path(__file__).resolve().parent.parent
parser=argparse.ArgumentParser()
parser.add_argument('--n8n-bin',required=True)
parser.add_argument('--user-folder',required=True)
parser.add_argument('--output',required=True)
parser.add_argument('--model',required=True)
parser.add_argument('--patch-only',action='store_true')
args=parser.parse_args()
STAMP=dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S')
OUT=Path(args.output).resolve()/('native-'+STAMP)
OUT.mkdir(parents=True,exist_ok=True)
PRIVATE=OUT/'private-logs'
PRIVATE.mkdir()
CALLS={}
LOCK=threading.Lock()
class Counter(BaseHTTPRequestHandler):
    def do_POST(self):
        size=int(self.headers.get('Content-Length','0'))
        if size>10000:self.send_error(413);return
        data=json.loads(self.rfile.read(size))
        with LOCK:CALLS.setdefault(self.path,[]).append(data)
        payload=json.dumps({'accepted':True,'synthetic_fixture':True}).encode()
        self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(payload)));self.end_headers();self.wfile.write(payload)
    def log_message(self,*args):pass

def node(name,kind,params,x,y,version=None):
    return {'id':name,'name':name,'type':'n8n-nodes-base.'+kind,'typeVersion':version or (4.2 if kind=='httpRequest' else 2 if kind=='code' else 1),'parameters':params,'position':[x,y]}
def workflow(case,port):
    nodes=[node('Manual','manualTrigger',{},0,0),node('A','code',{'jsCode':"return [{json:{id:'a1'}},{json:{id:'a2'}}];"},200,-100),node('B','code',{'jsCode':"return [{json:{id:'b1'}},{json:{id:'b2'}},{json:{id:'b3'}}];"},200,100)]
    edges={'Manual':{'main':[[{'node':'A','type':'main','index':0},{'node':'B','type':'main','index':0}]]}}
    connect=lambda a,b,i=0:edges.update({a:{'main':[[{'node':b,'type':'main','index':i}]]}})
    if case.startswith('merge'):
        nodes.append(node('Merge','merge',{'mode':'append','numberInputs':2},400,0,3.2));connect('A','Merge',0);connect('B','Merge',1);upstream='Merge'
    else:upstream=None
    if case!='merge_raw':
        nodes.append(node('Batch','code',{'mode':'runOnceForAllItems','jsCode':"const items=$input.all();return [{json:{batch_size:items.length,ids:items.map(x=>x.json.id)}}];"},600,0))
        if upstream:connect(upstream,'Batch')
        else:connect('A','Batch');connect('B','Batch')
        upstream='Batch'
    nodes.append(node('HTTP','httpRequest',{'method':'POST','url':f'http://127.0.0.1:{port}/{case}','sendBody':True,'specifyBody':'json','jsonBody':'={{ JSON.stringify($json) }}','options':{'timeout':10000}},800,0));connect(upstream,'HTTP')
    return {'id':'CompSetlyBench'+case,'name':'CompSetly synthetic fan-in '+case,'active':False,'nodes':nodes,'connections':edges,'settings':{'executionOrder':'v1','executionTimeout':60,'saveDataSuccessExecution':'none','saveDataErrorExecution':'none','saveManualExecutions':False}}

def completed(raw):
    decoder=json.JSONDecoder()
    for match in re.finditer(r'\{',raw):
        try:value,_=decoder.raw_decode(raw,match.start())
        except (ValueError,RecursionError):continue
        if isinstance(value,dict) and value.get('finished') is True and value.get('status')=='success':return value
    return None

server=ThreadingHTTPServer(('127.0.0.1',0),Counter)
thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
env=os.environ.copy();env.update(N8N_USER_FOLDER=str(Path(args.user_folder).resolve()),N8N_DIAGNOSTICS_ENABLED='false',N8N_VERSION_NOTIFICATIONS_ENABLED='false',N8N_LOG_LEVEL='info')
base=[shutil.which('node'),str(Path(args.n8n_bin).resolve())]
results=[]
try:
    cases=('patched_batch',) if '--patch-only' in sys.argv else ('direct_batch','merge_batch','merge_raw')
    for case in cases:
        wf=workflow('direct_batch' if case=='patched_batch' else case,server.server_port);wf['nodes'][-1]['parameters']['url']=f'http://127.0.0.1:{server.server_port}/{case}'
        file=OUT/(case+'.json');file.write_text(json.dumps(wf,indent=2)+'\n')
        if case=='patched_batch':
            script="import fs from 'node:fs';import {pathToFileURL} from 'node:url';const {insertAppend}=await import(pathToFileURL(process.argv[1]));const p=process.argv[2];const w=insertAppend(JSON.parse(fs.readFileSync(p,'utf8')),'Manual','Batch').workflow;fs.writeFileSync(p,JSON.stringify(w,null,2));"
            patched=subprocess.run([base[0],'--input-type=module','-e',script,str(Path(args.model).resolve()),str(file)],capture_output=True,text=True,timeout=20)
            if patched.returncode:raise RuntimeError('Own rewrite failed before native import: '+patched.stderr[:500])
            wf=json.loads(file.read_text())
        for action,args in [('import',['import:workflow','--input='+str(file)]),('execute',['execute','--id='+wf['id']])]:
            log=PRIVATE/(case+'-'+action+'.log')
            with log.open('w',encoding='utf-8') as output:run=subprocess.run(base+args,cwd=ROOT,env=env,stdout=output,stderr=subprocess.STDOUT,timeout=90)
            if run.returncode:raise RuntimeError(case+' '+action+' failed; inspect private log')
        execution=completed(log.read_text(encoding='utf-8',errors='replace'))
        if not execution:raise RuntimeError(case+' has no verified completion receipt')
        data=execution['data']['resultData']['runData'];calls=CALLS.get('/'+case,[])
        row={'case':case,'execution_order':'v1','native_completed':True,'http_node_runs':len(data.get('HTTP',[])),'batch_node_runs':len(data.get('Batch',[])),'actual_loopback_http_requests':len(calls),'batch_sizes':[c.get('batch_size') for c in calls if 'batch_size' in c],'synthetic_items_received':sum(c.get('batch_size',1) for c in calls),'workflow_sha256':hashlib.sha256(file.read_bytes()).hexdigest(),'external_requests':0}
        results.append(row);print(json.dumps(row),flush=True)
finally:
    server.shutdown();server.server_close();thread.join(timeout=5)
receipt={'checked_at':dt.datetime.now(dt.timezone.utc).isoformat(),'n8n_version':'2.45.4','synthetic_controlled_experiment':True,'server':'ephemeral loopback, stopped on completion','results':results,'not_traffic_or_external_provider_billing':True,'limit':'Only these fixtures were executed. Unknown user code, conditionals, loops, merge modes and provider batching require separate analysis.'}
(OUT/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')

print(json.dumps({'receipt':str(OUT/'receipt.json'),'cases_completed':len(results),'external_requests':0}),flush=True)
