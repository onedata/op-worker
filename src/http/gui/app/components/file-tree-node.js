import Ember from 'ember';

export default Ember.Component.extend({
  actions: {
    clickAction: function (file) {
      this.sendAction('clickAction', file);
    },

    // Components are nested, so we need to bubble the action up
    // until it reaches the controller.
    // @todo - can it be done better?
    fileClicked: function (file) {
      this.sendAction('clickAction', file);
    }
  }
});
