 import ShowPermissionsBase from './show-permissions-base';

/** Route to collection of single Group single Space permissions.
 *  Loads the userPermissions collection for Space loaded in space route.
 *  Renders to main-content.
 */
 export default ShowPermissionsBase.extend({
   permissionsType: 'group'
 });
