import DS from 'ember-data';
import PermissionsBase from './permissions-base';

/** A set of single Space permissions for a single User */
export default PermissionsBase.extend({
  user: DS.belongsTo('spaceUser', {async: true}),

  /** Common alias for owner - in this case group */
  owner: function() {
    return this.get('user');
  }.property('user')
});
