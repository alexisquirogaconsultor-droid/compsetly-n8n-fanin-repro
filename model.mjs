export const LIMITS={bytes:1000000,nodes:500,edges:2500,pathCount:1000};
const mergeType='n8n-nodes-base.merge';
const conditionalTypes=new Set(['n8n-nodes-base.if','n8n-nodes-base.switch','n8n-nodes-base.splitInBatches','n8n-nodes-base.wait','n8n-nodes-base.executeWorkflow']);
const rewriteTypes=new Set(['manualTrigger','scheduleTrigger','webhook','start','code','httpRequest','set','noOp','merge'].map(t=>'n8n-nodes-base.'+t));
export function parseWorkflow(text){
 if(typeof text!=='string'||new TextEncoder().encode(text).length>LIMITS.bytes)throw new Error('Use a workflow export smaller than 1 MB.');
 let w;try{w=JSON.parse(text);}catch{throw new Error('This file is not valid JSON.');}
 if(Array.isArray(w)){if(w.length!==1)throw new Error('Analyze one workflow at a time.');w=w[0];}
 if(!w||!Array.isArray(w.nodes)||!w.connections||typeof w.connections!=='object'||Array.isArray(w.connections))throw new Error('Export the workflow with both nodes and connections.');
 if(!w.nodes.length||w.nodes.length>LIMITS.nodes)throw new Error('Supported size: 1–500 nodes.');
 const names=new Set();for(const n of w.nodes){if(!n||typeof n.name!=='string'||!n.name||typeof n.type!=='string'||names.has(n.name))throw new Error('Node names must be present and unique.');names.add(n.name);}
 return w;
}
export function graph(w){
 const nodes=new Map(w.nodes.map(n=>[n.name,n])),edges=[],notModeled=[];
 for(const [source,kinds] of Object.entries(w.connections)){
  if(!nodes.has(source))throw new Error('A connection starts at a missing node.');
  if(!kinds||typeof kinds!=='object')throw new Error('Invalid connection map.');
  for(const [kind,outputs] of Object.entries(kinds)){
   if(kind!=='main'){notModeled.push('Non-main connection type: '+kind);continue;}
   if(!Array.isArray(outputs))throw new Error('Invalid main output list.');
   outputs.forEach((targets,output)=>{
    if(targets===null)return;
    if(!Array.isArray(targets))throw new Error('Invalid main output.');
    targets.forEach((e,slot)=>{
     if(!e||!nodes.has(e.node)||e.type!=='main'||!Number.isInteger(e.index)||e.index<0)throw new Error('A connection has a missing target or invalid input.');
     edges.push({source,target:e.node,input:e.index,output,slot});
    });
   });
  }
 }
 if(edges.length>LIMITS.edges)throw new Error('Supported size: at most 2,500 main connections.');
 const incoming=new Map(w.nodes.map(n=>[n.name,[]])),outgoing=new Map(w.nodes.map(n=>[n.name,[]]));
 for(const e of edges){incoming.get(e.target).push(e);outgoing.get(e.source).push(e);}
 const roots=w.nodes.filter(n=>!n.disabled&&(n.type.toLowerCase().includes('trigger')||n.type==='n8n-nodes-base.webhook'||n.type==='n8n-nodes-base.start')).map(n=>n.name);
 return {nodes,edges,incoming,outgoing,roots,notModeled:[...new Set(notModeled)]};
}
export function analyze(w,rootName){
 const g=graph(w),root=rootName||g.roots[0];
 if(!root||!g.roots.includes(root))throw new Error('Choose one trigger. Separate triggers are separate executions.');
 const reachable=new Set([root]),stack=[root];
 while(stack.length)for(const e of g.outgoing.get(stack.pop()))if(!reachable.has(e.target)){reachable.add(e.target);stack.push(e.target);}
 const otherReachable=new Set(g.roots.filter(n=>n!==root)),otherStack=[...otherReachable];
 while(otherStack.length)for(const e of g.outgoing.get(otherStack.pop()))if(!otherReachable.has(e.target)){otherReachable.add(e.target);otherStack.push(e.target);}
 const degree=new Map([...reachable].map(n=>[n,g.incoming.get(n).filter(e=>reachable.has(e.source)).length]));
 const ready=[...reachable].filter(n=>degree.get(n)===0),order=[];
 while(ready.length){const n=ready.shift();order.push(n);for(const e of g.outgoing.get(n)){if(!reachable.has(e.target))continue;degree.set(e.target,degree.get(e.target)-1);if(!degree.get(e.target))ready.push(e.target);}}
 if(order.length!==reachable.size)return {root,roots:g.roots,state:'cycle_requires_manual_review',candidates:[],warnings:['A reachable cycle or loop prevents a finite path analysis. No workflow rewrite is offered.']};
 const arrivals=new Map(),ancestors=new Map(),witnesses=new Map(),candidates=[];
 const warnings=[...g.notModeled];
 if(w.settings?.executionOrder!=='v1')warnings.push('Execution order is not explicitly v1; graphs are shown as review leads and rewrite is disabled.');
 for(const name of order){
  const n=g.nodes.get(name),ins=g.incoming.get(name).filter(e=>reachable.has(e.source));
  const uncertain=[...new Set(ins.flatMap(e=>ancestors.get(e.source)||[]))];
  if(conditionalTypes.has(n.type)||n.disabled||!rewriteTypes.has(n.type)||n.executeOnce||n.onError==='continueErrorOutput')uncertain.push(name+' (execution semantics need review)');
  if(otherReachable.has(name))uncertain.push(name+' (also reachable from another trigger)');
  let count=name===root?1:ins.reduce((sum,e)=>sum+(arrivals.get(e.source)||0),0);
  let paths=name===root?[[root]]:ins.flatMap(e=>(witnesses.get(e.source)||[]).map(p=>[...p,name])).slice(0,2);
  if(n.type===mergeType&&!n.disabled){
   const expected=Number(n.parameters?.numberInputs??2),inputs=new Set(ins.map(e=>e.input));
   if(n.parameters?.mode==='append'&&Number.isInteger(expected)&&expected>=2&&inputs.size===expected&&[...inputs].every(i=>i<expected)){
    if(ins.some(e=>(arrivals.get(e.source)||0)>1))uncertain.push(name+' (repeated Merge inputs)');
    else{count=1;paths=[[root,'[Append waits for '+expected+' inputs]',name]];}
   }else uncertain.push(name+' (unmodeled Merge mode or missing inputs)');
  }
  count=Math.min(LIMITS.pathCount,count);arrivals.set(name,count);ancestors.set(name,[...new Set(uncertain)]);witnesses.set(name,paths);
  const parents=[...new Set(ins.map(e=>e.source))];
  if(count>1&&parents.length>1&&n.type!==mergeType){
   const supported=w.settings?.executionOrder==='v1'&&!n.disabled&&!uncertain.length&&ins.every(e=>e.input===0)&&ins.length===parents.length&&ins.every(e=>g.outgoing.get(e.source).length===1)&&g.incoming.get(name).length===ins.length;
   candidates.push({node:name,type:n.type,parent_nodes:parents,possible_arrivals:count,witness_paths:paths,uncertain_nodes:[...new Set(uncertain)],rewrite_available:supported,scope:'Potential main-branch arrivals if each branch emits data. This is not a count of HTTP requests or guaranteed runtime executions.'});
  }
 }
 return {root,roots:g.roots,state:'review',candidates,node_arrivals:Object.fromEntries(arrivals),warnings,limits:['Input code is never executed. Empty branches, conditions, loops, retries, sub-workflows, item pairing and API batching can change behavior.','Append waits for connected inputs and keeps their items. One HTTP node run can still send one request per item.','Native benchmark covers only the published synthetic fixtures on n8n 2.45.4.']};
}
export function insertAppend(w,rootName,targetName,idFactory=()=>crypto.randomUUID()){
 const report=analyze(w,rootName),candidate=report.candidates.find(c=>c.node===targetName);
 if(!candidate?.rewrite_available)throw new Error('This convergence requires manual review; an automatic Append rewrite is not offered.');
 const clone=JSON.parse(JSON.stringify(w)),g=graph(clone),target=g.nodes.get(targetName),ins=g.incoming.get(targetName).filter(e=>candidate.parent_nodes.includes(e.source));
 let name='Append before '+targetName,serial=1;while(g.nodes.has(name))name='Append before '+targetName+' '+serial++;
 const position=Array.isArray(target.position)?[Number(target.position[0])-220,Number(target.position[1])]:[300,300];
 clone.nodes.push({id:idFactory(),name,type:mergeType,typeVersion:3.2,position,parameters:{mode:'append',numberInputs:ins.length}});
 ins.forEach((e,index)=>{const edge=clone.connections[e.source].main[e.output][e.slot];edge.node=name;edge.index=index;});
 clone.connections[name]={main:[[{node:targetName,type:'main',index:0}]]};
 clone.id=idFactory();clone.name=(clone.name||'Workflow')+' · review Append before '+targetName;clone.active=false;
 for(const key of ['activeVersion','activeVersionId','versionId','createdAt','updatedAt','versionMetadata','pinData','staticData'])delete clone[key];
 return {workflow:clone,plan:{inserted_node:name,target:targetName,inputs:ins.length,new_workflow_id:true,inactive:true,warnings:['Append concatenates items; it does not batch HTTP requests by itself.','Test empty branches and downstream item pairing. Inspect any existing webhook path before publishing the clone.','The clone retains your node parameters and credential references. Keep the downloaded file private.']}};
}
