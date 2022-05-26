%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Common definitions for modules operating on file meta forest.
%%% @end
%%%--------------------------------------------------------------------
-ifndef(FILE_META_FOREST_HRL).
-define(FILE_META_FOREST_HRL, 1).

-record(list_extended_info, {
    is_finished :: boolean(),
    datastore_token :: file_meta_forest:datastore_list_token() | undefined,
    last_name :: file_meta_forest:last_name() | undefined,
    last_tree :: file_meta_forest:last_tree() | undefined
}).

-endif.
