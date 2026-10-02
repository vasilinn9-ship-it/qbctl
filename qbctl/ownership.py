"""Lexical Windows ownership checks. Never inspect completed payload."""
from pathlib import Path
from bisect import bisect_left
from .common import Fault, lexical, norm

def key(path):
    value=norm(path).casefold()
    return value[:-4] if value.endswith('.!qb') else value

def overlaps(a,b):
    a,b=key(a),key(b)
    return a==b or a.startswith(b+'\\') or b.startswith(a+'\\')

def claims(root,files,field='name'):
    result=[]
    for f in files:
        if f.get('is_padding') or f.get('padding') or 'p' in f.get('attr',''): continue
        name=f.get(field)
        if isinstance(name,list): name='/'.join(name)
        if not isinstance(name,str) or not name: raise Fault('METADATA_MISSING','Нет точного пути файла для проверки владения.')
        p=lexical(root,name); relative=p.relative_to(Path(root).absolute())
        result.append({'file':str(p),'tree':str(Path(root)/relative.parts[0]) if len(relative.parts)>1 else None})
    return result

def conflicts(left,right):
    out=[]; files=sorted({key(b['file']) for b in right}); trees=sorted({key(b['tree']) for b in right if b.get('tree')})
    def match(value,items):
        k=key(value); i=bisect_left(items,k)
        if i<len(items) and (items[i]==k or items[i].startswith(k+'\\')): return True
        parts=k.split('\\')
        for n in range(1,len(parts)):
            parent='\\'.join(parts[:n]); j=bisect_left(items,parent)
            if j<len(items) and items[j]==parent: return True
        return False
    for a in left:
        if match(a['file'],files) or (a.get('tree') and match(a['tree'],trees)): out.append(a['file'])
    return out

def scope(op):
    hashes=set(op.get('aliases',[]))|{op['hash']}
    if op.get('other_hash'): hashes.add(op['other_hash'])
    hashes.update(t['hash'] for t in op.get('tasks',[]))
    global_scope=op['kind']=='control' and (bool(op.get('preferences') or op.get('changes')) or not op.get('tasks'))
    paths=list(op.get('payload_paths',[]))
    if op['kind']=='complete': paths.extend(p for f in op['files'] for p in (f['source'],f['destination']))
    return {'hashes':hashes,'paths':paths,'global':global_scope}
