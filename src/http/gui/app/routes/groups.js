import Ember from 'ember';

export default Ember.Route.extend({
  commonModals: Ember.inject.service(),
  i18n: Ember.inject.service(),

  beforeModel(transition) {
    let i18n = this.get('i18n');
    this.get('commonModals').openInfoModal(
      i18n.t('common.featureNotSupportedShort'),
      i18n.t('common.featureNotSupportedLong')
    );
    transition.abort();
  }
});
