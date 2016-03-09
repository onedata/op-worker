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
    // TODO: s.get('isDefault')
    let defaultSpace = this.modelFor('data').find((s) => s.isDefault);

    // TODO: get(id)
    if (defaultSpace) {
      console.debug(`Redirecting to default space: ${defaultSpace.id}`);
      this.transitionTo('data.data-space', defaultSpace.id);
    } else {
      console.error(`There is no default space!`);
    }
  },
});
