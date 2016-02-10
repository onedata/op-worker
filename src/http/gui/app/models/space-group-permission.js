import DS from 'ember-data';
import PermissionsBase from './base/permissions-base';

/**
 * A set of single Space permissions for a single Group
 * @module models/space-group-permission
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */
export default PermissionsBase.extend({
  group: DS.belongsTo('spaceGroup', {async: true}),

  /** Common alias for owner - in this case group */
  owner: function() {
    return this.get('group');
  }.property('group')
});
