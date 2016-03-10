import Ember from 'ember';

export default Ember.Component.extend({
  actions: {
    toggleDir(dirFile) {
      dirFile.set('isExpanded', !dirFile.get('isExpanded'));
    }
  }
});
