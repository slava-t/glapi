const stringify = require('safe-stable-stringify');
const adjustError = function(err) {
  if (typeof err === 'object' && (err.response || err.request || err.config)) {
    const result = stringify(err, null, 2);
    if (result.toLowerCase().indexOf('axios') >= 0) {
      return result;
    }
  }
  return err;
};

module.exports = {
  adjustError
};
