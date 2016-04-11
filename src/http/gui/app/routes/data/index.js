/**
 * Redirects to a default space, as the empty view with selected space
 * doesn't make sense.
 *
 * @module routes/data/index
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */

import Ember from 'ember';

export default Ember.Route.extend({
  afterModel() {
    let defaultSpace = this.modelFor('data').find((s) => s.get('isDefault'));
    if (defaultSpace) {
      let dsId = defaultSpace.get('id');
      console.debug(`Redirecting to default space: ${dsId}`);
      this.transitionTo('data.data-space', dsId);
    } else {
      // TODO: go to first space in collection?
      console.error(`There is no default space!`);
    }
  },
});
