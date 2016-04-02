import DS from 'ember-data';

export default DS.Model.extend({
  provider: DS.belongsTo('provider', {async: true}),
  blocks: DS.attr()
});
