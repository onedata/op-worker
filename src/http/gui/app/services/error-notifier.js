import Ember from 'ember';

export default Ember.Service.extend({
  handle(message) {
    if (message.status === 'error') {
      window.alert(message.message);
    } else {
      // TODO
    }
  }
});
