# encoding: utf-8
require "azure/storage"

module Backup
  module Storage
    class AzureStore < Base
      include Storage::Cycler
      class Error < Backup::Error; end

      attr_accessor :storage_account, :storage_access_key, :container_name, :chunk_size, :retry_count, :retry_interval

      def initialize(model, storage_id = nil)
        super
        @path           ||= "backups"
        @chunk_size     ||= 1024 * 1024 * 4 # 4 MB
        @retry_count    ||= 3
        @retry_interval ||= 30
        path.sub!(%r{^/}, "")

        check_configuration

        Azure::Storage.setup(storage_account_name: storage_account, storage_access_key: storage_access_key)
      end

      def blob_service_with_retry_filter
        @blob_service_with_retry_filter = Azure::Storage::Blob::BlobService.new
        @blob_service_with_retry_filter.with_filter(Azure::Storage::Core::Filter::LinearRetryPolicyFilter.new(@retry_count, @retry_interval))
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
          Logger.info "Storage::AzureStore uploading '#{container.name}/#{destination_path}'"

          # https://github.com/Azure/azure-storage-ruby/issues/15
          # blob_service.create_block_blob(container.name, destination_path, ::File.open(source_path, "rb", &:read))

          # Following: https://github.com/Azure-Samples/storage-blob-ruby-getting-started/blob/master/blobs_advanced.rb#L132
          blocks = []
          File.open(source_path, 'rb') do |file|
            while (file_bytes = file.read(@chunk_size))
              block_id = Base64.strict_encode64(SecureRandom.uuid)
              Logger.info "Storage::AzureStore uploading '#{container.name}/#{destination_path} - block: #{block_id}'"
              blob_service.put_blob_block(container_name, destination_path, block_id, file_bytes)
              blocks << [block_id]
            end
          end

          Logger.info "Storage::AzureStore finalizing upload"
          blob_service.commit_blob_blocks(container_name, destination_path, blocks)
        end
      end

      # Called by the Cycler.
      # Any error raised will be logged as a warning.
      def remove!(package)
        Logger.info "Removing backup package dated #{package.time}..."

        package.filenames.each do |filename|
          destination_path = "#{remote_path_for(package)}/#{filename}"
          Logger.info "Storage::AzureStore deleting '#{destination_path}'"
          blob_service.delete_blob(container.name, destination_path)
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
