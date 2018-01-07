router = require('router').new()
print('inside app')

router:get('/', function()
  app.response:write('hello app')
end)

router:get('/bar', function()
  app.response:write('bar')
end)

router:run()
