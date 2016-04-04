/**
 * Route to collection of single user single space permissions.
 * Loads the userPermissions collection for Space loaded in space route.
 *
 * @module routes/spaces/show/users
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */

import ShowPermissionsBase from './show-permissions-base';

export default ShowPermissionsBase.extend({
  permissionsType: 'user',

  actions: {
    inviteItem() {
      let space = this.modelFor('spaces.show');
      this.get('oneproviderServer').inviteUser(space).then(
        (token) => {
          this.set('inviteToken', token);
        }
        // TODO: handle errors
      );
    }
  }
});
