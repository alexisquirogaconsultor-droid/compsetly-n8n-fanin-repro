import {test} from 'node:test';
import assert from 'node:assert/strict';
import {parseWorkflow,analyze,insertAppend} from '../model.mjs';
const edge=node=>({main:[[{node,type:'main',index:0}]]});
function fixture(){return {id:'original-id',name:'Original',active:true,activeVersionId:'live-version',settings:{executionOrder:'v1'},nodes:[{name:'Manual',id:'m',type:'n8n-nodes-base.manualTrigger'},{name:'A',id:'a',type:'n8n-nodes-base.code',parameters:{jsCode:'globalThis.DO_NOT_EXECUTE=true;'}},{name:'B',id:'b',type:'n8n-nodes-base.code'},{name:'Batch',id:'c',type:'n8n-nodes-base.code',parameters:{mode:'runOnceForAllItems'}},{name:'HTTP',id:'h',type:'n8n-nodes-base.httpRequest',credentials:{httpHeaderAuth:{id:'private-reference'}},parameters:{url:'https://example.test/'}}],connections:{Manual:{main:[[{node:'A',type:'main',index:0},{node:'B',type:'main',index:0}]]},A:edge('Batch'),B:edge('Batch'),Batch:edge('HTTP')}};}
test('same-trigger convergence yields a review candidate; input code stays data',()=>{
 const w=fixture(),r=analyze(w,'Manual');assert.equal(r.candidates[0].node,'Batch');assert.equal(r.candidates[0].possible_arrivals,2);assert.equal(r.node_arrivals.HTTP,2);assert.equal(globalThis.DO_NOT_EXECUTE,undefined);
});
test('review clone adds a real Append barrier, keeps original bytes and creates a new inactive workflow',()=>{
 const w=fixture(),original=JSON.stringify(w);let id=0;const {workflow,plan}=insertAppend(w,'Manual','Batch',()=>String(++id));
 assert.equal(JSON.stringify(w),original);assert.notEqual(workflow.id,w.id);assert.equal(workflow.active,false);assert.equal(workflow.activeVersionId,undefined);assert.deepEqual(workflow.nodes.find(n=>n.name==='HTTP').credentials,w.nodes.find(n=>n.name==='HTTP').credentials);
 assert.equal(analyze(workflow,'Manual').node_arrivals.HTTP,1);assert.equal(plan.inputs,2);assert.equal(workflow.nodes.find(n=>n.type==='n8n-nodes-base.merge').parameters.mode,'append');
});
test('independent Manual and Schedule triggers are not counted as simultaneous branches',()=>{
 const w=fixture();w.nodes.push({name:'Schedule',id:'s',type:'n8n-nodes-base.scheduleTrigger'});w.connections.Manual=edge('A');w.connections.Schedule=edge('B');
 assert.equal(analyze(w,'Manual').candidates.length,0);assert.equal(analyze(w,'Schedule').candidates.length,0);
});
test('conditional ancestry, legacy execution order and cycles prevent a suggested rewrite',()=>{
 let w=fixture();w.nodes.find(n=>n.name==='A').type='n8n-nodes-base.if';assert.equal(analyze(w).candidates[0].rewrite_available,false);assert.throws(()=>insertAppend(w,'Manual','Batch'));
 w=fixture();delete w.settings;assert.equal(analyze(w).candidates[0].rewrite_available,false);
 w=fixture();w.connections.HTTP=edge('A');assert.equal(analyze(w).state,'cycle_requires_manual_review');
});
test('an Append node is not reported as proof of fewer HTTP requests',()=>{
 const w=insertAppend(fixture(),'Manual','Batch',()=>crypto.randomUUID()).workflow,r=analyze(w,'Manual');assert.equal(r.node_arrivals.HTTP,1);assert.ok(r.limits.some(l=>l.includes('one request per item')));
});
test('rewriting one trigger cannot silently stall another trigger or bypass unmodeled execution semantics',()=>{
 const w=fixture();w.nodes.push({name:'Schedule',id:'s',type:'n8n-nodes-base.scheduleTrigger'});w.connections.Schedule=edge('A');
 assert.equal(analyze(w,'Manual').candidates[0].rewrite_available,false);assert.throws(()=>insertAppend(w,'Manual','Batch'));
 const custom=fixture();custom.nodes.find(n=>n.name==='A').type='custom.specialRouter';assert.equal(analyze(custom).candidates[0].rewrite_available,false);
 const disabled=fixture();disabled.nodes.find(n=>n.name==='A').disabled=true;assert.equal(analyze(disabled).candidates[0].rewrite_available,false);
});
test('malformed exports, duplicate names, missing references and oversized inputs stop before analysis',()=>{
 assert.throws(()=>parseWorkflow('{'));assert.throws(()=>parseWorkflow(' '.repeat(1000001)));const w=fixture();w.nodes[1].name='Manual';assert.throws(()=>parseWorkflow(JSON.stringify(w)));
 const missing=fixture();missing.connections.A=edge('Missing');assert.throws(()=>analyze(missing));
});
