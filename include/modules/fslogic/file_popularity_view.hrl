%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Macros and records used by file_popularity_view module
%%% @end
%%%-------------------------------------------------------------------

-define(VIEW_NAME(SpaceId), <<"file-popularity-", SpaceId/binary>>).

% this record is used to define the starting row for a query on the
% file-popularity view
-record(index_token, {
    % doc_id of the last returned row
    % if defined it will be used with start_key to start the query
    % from the previously finished row
    last_doc_id :: undefined | binary(),
    % start_key, it is updated with the key of the last returned row
    % it is used (with last_doc_id) to start the query
    % from the previously finished row
    start_key :: undefined | [non_neg_integer()]
}).