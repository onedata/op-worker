import Ember from 'ember';

export default Ember.Route.extend({
  model: function () {
    return this.store.findAll('file');
  },

  actions: {
    createNewFile: function(name, type, parentID) {
      var file = this.get('store').createRecord('file', {
        name: name,
        type: type,
        parentId: parentID
      });
      file.save();
    }
  },
});
