import Ember from 'ember';

export default Ember.Component.extend({
  i18n: Ember.inject.service(),
  oneproviderServer: Ember.inject.service(),
  notify: Ember.inject.service(),

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

  formElementId: function() {
    return `invite-form-${this.get('modalId')}`;
  }.property('modalId'),

  clipboardTarget: function() {
    return `#${this.get('formElementId')} input`;
  }.property('formElementId'),

  selectTokenText() {
    let input = $('#invite-token-field')[0];
    $(input).focus();
    input.setSelectionRange(0, input.value.length);
  },

  actions: {
    getToken() {
      let type = this.get('type');
      this.get('oneproviderServer').getToken(type, this.get('space.id')).then(
        (token) => {
          this.set('inviteToken', token);
        },
        (error) => {
          // this.set('errorMessage', error);
          console.error(`Token ${type} fetch failed: ` + JSON.stringify(error));
        }
      );
    },
    selectAll() {
      this.selectTokenText();
    },
    closeModal() {
      this.set('inviteToken', null);
      this.set('errorMessage', null);
    },
    copySuccess() {
      this.selectTokenText();
      this.get('notify').info(this.get('i18n').t('common.notify.clipboardSuccess'));
    },
    copyError() {
      this.selectTokenText();
      this.get('notify').warn(this.get('i18n').t('common.notify.clipboardFailure'));
    },
  }
});
