local ret1 = require2('github.com/tsileo/gluarequire2/_tests/testmod')

function return2()
  assert(ret1.return1() == 1)
  return 2
end

return { return2 = return2 }
