-- Used as a "match func" when searching within a FileTree tree
return function(node, contents)
  if {{.expr}} then return true else return false end
end
