import Ember from 'ember';

export default Ember.Service.extend({
  handle(errorEvent) {
    if (errorEvent.severity === 'warning') {
      console.error('[ERROR-NOTIFIER] ' + errorEvent.message);
    } else if (errorEvent.severity === 'error') {
      window.alert('[ERROR-NOTIFIER] ' + errorEvent.message);
    } else if (errorEvent.severity === 'critical') {
      window.alert('[ERROR-NOTIFIER] CRITICAL: ' + errorEvent.message);
    }
  }
});
