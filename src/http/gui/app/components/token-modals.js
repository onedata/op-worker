import Ember from 'ember';

export default Ember.Component.extend({
  i18n: Ember.inject.service(),
  oneproviderServer: Ember.inject.service(),

  /** Current space that modals will use to act */
  space: null,

  /** Allowed: user, group, support */
  type: null,

  inviteToken: null,

  /** Id of HTML element with -modal suffix */
  modalId: function() {
    return `token-${this.get('type')}`;
  }.property('type'),

  modalTitle: function() {
    return this.get('i18n').t(`components.tokenModals.${this.get('type')}.title`);
  }.property('type'),

  modalLabel: function() {
    return this.get('i18n').t(`components.tokenModals.${this.get('type')}.label`);
  }.property('type'),

  actions: {
    getToken() {
      let type = this.get('type');
      this.get('oneproviderServer').getToken(type, this.get('space.id')).then(
        (token) => {
          this.set('inviteToken', token);
        },
        (error) => {
          this.set('errorMessage', error);
          console.error(`Token ${type} fetch failed: ` + JSON.stringify(error));
        }
      );
    }
  }
});
