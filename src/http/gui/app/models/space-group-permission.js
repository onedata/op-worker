import DS from 'ember-data';
import PermissionsBase from './permissions-base';

/** A set of single Space permissions for a single Group */
export default PermissionsBase.extend({
  group: DS.belongsTo('spaceGroup', {async: true}),

  /** Common alias for owner - in this case group */
  owner: function() {
    return this.get('group');
  }.property('group')
});
