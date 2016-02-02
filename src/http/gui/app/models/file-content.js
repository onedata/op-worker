import DS from 'ember-data';

export default DS.Model.extend({
  bytes: DS.attr('string'),
  file: DS.belongsTo('file')
});
