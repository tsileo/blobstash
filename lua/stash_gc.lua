local msgpack = require('msgpack')
local kvstore = require('kvstore')
local blobstore = require('blobstore')
local node = require('node')
 
-- Setup the `mark_kv` and `mark_filetree` global helper for the GC API
function mark_kv (key, version)
  local h = kvstore.get_meta_blob(key, version)
  if h ~= nil then
    mark(h)
    local _, ref, _ = kvstore.get(key, version)
    if ref ~= '' then
      mark(ref)
    end
  end
 end
 _G.mark_kv = mark_kv

function mark_filetree_node (ref)
  local data = blobstore.get(ref)
  local cnode = node.decode(data)
  if cnode.t == 'dir' then
    if cnode.r then
      for _, childRef in ipairs(cnode.r) do
        mark_filetree_node(childRef)
      end
    end
  else
    if cnode.r then
      for _, contentRef in ipairs(cnode.r) do
        mark(contentRef[2])
      end
    end
  end
  -- only mark the final ref once all the "data" blobs has been saved
  mark(ref)
end
_G.mark_filetree_node = mark_filetree_node
