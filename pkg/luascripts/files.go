package luascripts

// Autogenerated ; DO NOT EDIT

var files = map[string]string{
	"docstore_query.lua":       "-- Python-like string.split implementation http://lua-users.org/wiki/SplitJoin\nfunction string:split(sSeparator, nMax, bRegexp)\n   assert(sSeparator ~= '')\n   assert(nMax == nil or nMax >= 1)\n\n   local aRecord = {}\n\n   if self:len() > 0 then\n      local bPlain = not bRegexp\n      nMax = nMax or -1\n\n      local nField, nStart = 1, 1\n      local nFirst,nLast = self:find(sSeparator, nStart, bPlain)\n      while nFirst and nMax ~= 0 do\n         aRecord[nField] = self:sub(nStart, nFirst-1)\n         nField = nField+1\n         nStart = nLast+1\n         nFirst,nLast = self:find(sSeparator, nStart, bPlain)\n         nMax = nMax-1\n      end\n      aRecord[nField] = self:sub(nStart)\n   end\n\n   return aRecord\nend\nfunction get_path (doc, q)\n  q = q:gsub('%[%d', '.%1')\n  local parts = q:split('.')\n  p = doc\n  for _, part in ipairs(parts) do\n    if type(p) ~= 'table' then\n      return nil\n    end\n    if part:sub(1, 1) == '[' then\n      part = part:sub(2, 2)\n    end\n    if tonumber(part) ~= nil then\n      p = p[tonumber(part)]\n    else\n      p = p[part]\n    end\n    if p == nil then\n      return nil\n    end\n  end\n  return p\nend\n_G.get_path = get_path\nfunction in_list (doc, path, value, q)\n  local p = get_path(doc, path)\n  if type(p) ~= 'table' then\n    return false\n  end\n  for _, item in ipairs(p) do\n    if q == nil then\n      if item == value then return true end\n    else\n      if get_path(item, q) == value then return true end\n    end\n  end\n  return false\nend\n_G.in_list = in_list\n\nfunction match (doc, path, op, value)\n  p = get_path(doc, path)\n  if type(p) ~= type(value) then return false end\n  if op == 'EQ' then\n    return p == value\n  elseif op == 'NE' then\n    return p ~= value\n  elseif op == 'GT' then\n    return p > value\n  elseif op == 'GE' then\n    return p >= value\n  elseif op == 'LT' then\n    return p < value\n  elseif op == 'LE' then\n    return p <= value\n  end\n  return false\nend\n_G.match = match\n",
	"filetree_expr_search.lua": "-- Used as a \"match func\" when searching within a FileTree tree\nreturn function(node, contents)\n  if {{.expr}} then return true else return false end\nend\n",
	"stash_gc.lua":             "local msgpack = require('msgpack')\nlocal kvstore = require('kvstore')\nlocal blobstore = require('blobstore')\nlocal node = require('node')\n \nfunction premark_kv (key, version)\n  local h = kvstore.get_meta_blob(key, version)\n  if h ~= nil then\n    local _, ref, _ = kvstore.get(key, version)\n    if ref ~= '' then\n      premark(ref)\n    end\n    premark(h)\n  end\n end\n _G.premark_kv = premark_kv\n\nfunction premark_filetree_node (ref)\n  local data = blobstore.get(ref)\n  local cnode = node.decode(data)\n  if cnode.t == 'dir' then\n    if cnode.r then\n      for _, childRef in ipairs(cnode.r) do\n        premark_filetree_node(childRef)\n      end\n    end\n  else\n    if cnode.r then\n      for _, contentRef in ipairs(cnode.r) do\n        premark(contentRef[2])\n      end\n    end\n  end\n  -- only mark the final ref once all the \"data\" blobs has been saved\n  premark(ref)\nend\n_G.premark_filetree_node = premark_filetree_node\n \n-- Setup the `mark_kv` and `mark_filetree` global helper for the GC API\nfunction mark_kv (key, version)\n  local h = kvstore.get_meta_blob(key, version)\n  if h ~= nil then\n    local _, ref, _ = kvstore.get(key, version)\n    if ref ~= '' then\n      mark(ref)\n    end\n    mark(h)\n  end\n end\n _G.mark_kv = mark_kv\n\nfunction mark_filetree_node (ref)\n  local data = blobstore.get(ref)\n  local cnode = node.decode(data)\n  if cnode.t == 'dir' then\n    if cnode.r then\n      for _, childRef in ipairs(cnode.r) do\n        mark_filetree_node(childRef)\n      end\n    end\n  else\n    if cnode.r then\n      for _, contentRef in ipairs(cnode.r) do\n        mark(contentRef[2])\n      end\n    end\n  end\n  -- only mark the final ref once all the \"data\" blobs has been saved\n  mark(ref)\nend\n_G.mark_filetree_node = mark_filetree_node\n",
	"test.lua":                 "return function()\n    return {{.expr}}\nend\n",
}
