# encoding: utf-8
require "azure/storage"

module Backup
  module Storage
    class AzureStore < Base
      include Storage::Cycler
      class Error < Backup::Error; end

      # Azure credentials
      attr_accessor :storage_account, :storage_access_key

      # Azure Storage Container
      attr_accessor :container_name, :container
      attr_accessor :blob_service, :chunk_size

      def initialize(model, storage_id = nil)
        super
        @path       ||= "backups"
        @chunk_size ||= 1024 * 1024 * 4 # bytes
        path.sub!(%r{^/}, "")

        check_configuration

        Azure::Storage.setup(storage_account_name: storage_account, storage_access_key: storage_access_key)
      end

      def blob_service_with_retry_filter
        @blob_service_with_retry_filter = Azure::Blob::BlobService.new
        @blob_service_with_retry_filter.with_filter(Azure::Storage::Core::Filter::LinearRetryPolicyFilter.new)
        @blob_service_with_retry_filter
      end

      def blob_service
        @blob_service ||= blob_service_with_retry_filter
      end

      def container
        @container ||= blob_service.get_container_properties(container_name)
      end

      def transfer!
        package.filenames.each do |filename|
          source_path = File.join(Config.tmp_path, filename)
          destination_path = File.join(remote_path, filename)
          Logger.info "Creating Block Blob '#{container.name}/#{destination_path}'..."

          blobs.create_block_blob(container.name, destination_path, ::File.open(source_path, 'rb') { |file| file.read })

          # blob = blob_service.create_block_blob(container.name, dest, "")
          # chunk_ids = []
          #
          # File.open(src, "r") do |fh_in|
          #   until fh_in.eof?
          #     chunk = sprintf("%05d", (fh_in.pos / chunk_size))
          #     Logger.info "Storing blob '#{blob.name}/#{chunk}'..."
          #     blob_service.create_blob_block(container.name, blob.name, chunk, fh_in.read(chunk_size))
          #     chunk_ids.push([chunk])
          #   end
          # end
          # blob_service.commit_blob_blocks(container.name, blob.name, chunk_ids)
        end
      end

      # Called by the Cycler.
      # Any error raised will be logged as a warning.
      def remove!(package)
        Logger.info "Removing backup package dated #{package.time}..."

        package.filenames.each do |filename|
          blob_service.delete_blob(container.name, "#{remote_path_for(package)}/#{filename}")
        end
      end

      def check_configuration
        required = %w(storage_account storage_access_key container_name)

        raise Error, <<-EOS if required.map { |name| send(name) }.any?(&:nil?)
          Configuration Error
          #{required.map { |name| "##{name}" }.join(", ")} are all required
        EOS
      end
    end
  end
end
