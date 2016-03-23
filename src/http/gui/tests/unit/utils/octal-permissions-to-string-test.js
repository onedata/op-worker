import octalPermissionsToString from '../../../utils/octal-permissions-to-string';
import { module, test } from 'qunit';

module('Unit | Utility | octal permissions to string');

let perms = {
  644: 'rw-r--r--',
  755: 'rwxr-xr-x',
  112: '--x--x-w-',
  334: '-wx-wxr--',
  567: 'r-xrw-rwx',
  777: 'rwxrwxrwx',
};

for (let octal in perms) {
  // @todo remove this suppresion when the test is written in a better way
  // https://jslinterrors.com/dont-make-functions-within-a-loop
  /*jshint -W083 */
  test('perms ' + octal, function(assert) {
    assert.equal(octalPermissionsToString(octal), perms[octal]);
  });
}
