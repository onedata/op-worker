/**
 * Route to collection of single Group single Space permissions.
 * Loads the userPermissions collection for Space loaded in space route.
 *
 * @module routes/spaces/show/groups
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */

import ShowPermissionsBase from './show-permissions-base';


export default ShowPermissionsBase.extend({
 permissionsType: 'group',

 actions: {
   inviteItem() {
     let space = this.modelFor('spaces.show');
     this.get('oneproviderServer').inviteGroup(space).then(
       (token) => {
         this.set('inviteToken', token);
       }
       // TODO: handle errors
     );
   }
 }
});
