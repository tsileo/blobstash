-- Python-like string.split implementation http://lua-users.org/wiki/SplitJoin
function string:split(sSeparator, nMax, bRegexp)
   assert(sSeparator ~= '')
   assert(nMax == nil or nMax >= 1)

   local aRecord = {}

   if self:len() > 0 then
      local bPlain = not bRegexp
      nMax = nMax or -1

      local nField, nStart = 1, 1
      local nFirst,nLast = self:find(sSeparator, nStart, bPlain)
      while nFirst and nMax ~= 0 do
         aRecord[nField] = self:sub(nStart, nFirst-1)
         nField = nField+1
         nStart = nLast+1
         nFirst,nLast = self:find(sSeparator, nStart, bPlain)
         nMax = nMax-1
      end
      aRecord[nField] = self:sub(nStart)
   end

   return aRecord
end
function get_path (doc, q)
  q = q:gsub('%[%d', '.%1')
  local parts = q:split('.')
  p = doc
  for _, part in ipairs(parts) do
    if type(p) ~= 'table' then
      return nil
    end
    if part:sub(1, 1) == '[' then
      part = part:sub(2, 2)
    end
    if tonumber(part) ~= nil then
      p = p[tonumber(part)]
    else
      p = p[part]
    end
    if p == nil then
      return nil
    end
  end
  return p
end
_G.get_path = get_path
function in_list (doc, path, value, q)
  local p = get_path(doc, path)
  if type(p) ~= 'table' then
    return false
  end
  for _, item in ipairs(p) do
    if q == nil then
      if item == value then return true end
    else
      if get_path(item, q) == value then return true end
    end
  end
  return false
end
_G.in_list = in_list

function match (doc, path, op, value)
  p = get_path(doc, path)
  if type(p) ~= type(value) then return false end
  if op == 'EQ' then
    return p == value
  elseif op == 'NE' then
    return p ~= value
  elseif op == 'GT' then
    return p > value
  elseif op == 'GE' then
    return p >= value
  elseif op == 'LT' then
    return p < value
  elseif op == 'LE' then
    return p <= value
  end
  return false
end
_G.match = match
