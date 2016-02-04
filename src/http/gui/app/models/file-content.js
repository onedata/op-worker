// This is a prototype model representing file content in file browser.

import DS from 'ember-data';

export default DS.Model.extend({
  bytes: DS.attr('string'),
  file: DS.belongsTo('file')
});
