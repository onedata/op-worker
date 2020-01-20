%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Behaviour that should be implemented by modules using
%%% replica_deletion mechanism.
%%% @end
%%%-------------------------------------------------------------------
-module(replica_deletion_behaviour).
-author("Jakub Kudzia").

%%%===================================================================
%%% Callbacks
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% This function is called by replica_deletion_worker to ensure
%% whether it should delete file replica. The check is performed right
%% before deleting the file.
%% @end
%%-------------------------------------------------------------------
-callback replica_deletion_predicate(od_space:id(), replica_deletion:job_id()) -> boolean().

%%-------------------------------------------------------------------
%% @doc
%% Posthook executed by replica_deletion_worker after deleting file
%% replica.
%% @end
%%-------------------------------------------------------------------
-callback process_replica_deletion_result(replica_deletion:result(), od_space:id(),
    file_meta:uuid(), replica_deletion:job_id()) -> ok.

-optional_callbacks([replica_deletion_predicate/2]).