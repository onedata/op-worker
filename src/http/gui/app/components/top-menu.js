// Prototype ember controller for the component top-menu.
// Here user actions are handled.

import Ember from 'ember';

export default Ember.Component.extend({
  actions: {
    clickAction: function () {
      console.log('clickAction');
      this.sendAction('clickAction');
    }
  }
});
