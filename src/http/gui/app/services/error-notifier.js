import Ember from 'ember';

export default Ember.Service.extend({
  notify: Ember.inject.service('notify'),
  i18n: Ember.inject.service('i18n'),

  handle(errorEvent) {
    if (errorEvent.severity === 'warning') {
      this.get('notify').warning(errorEvent.message);
      console.error('[ERROR-NOTIFIER] ' + errorEvent.message);
    } else if (errorEvent.severity === 'error') {
      this.get('notify').error(errorEvent.message, {
        closeAfter: null
      });
      window.alert('[ERROR-NOTIFIER] ' + errorEvent.message);
    } else if (errorEvent.severity === 'critical') {
      // TODO: there are info, success, warning, alert and error in notify
      this.get('notify').alert(errorEvent.message, {
        closeAfter: null
      });
      window.alert('[ERROR-NOTIFIER] CRITICAL: ' + errorEvent.message);
    }
  }
});
