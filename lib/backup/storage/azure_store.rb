# encoding: utf-8
require 'fog/azurerm'

module Backup
  module Storage
    class AzureStore < Base
      include Storage::Cycler
      class Error < Backup::Error; end

      attr_accessor :storage_account, :storage_access_key, :container_name, :cloud_environment

      def initialize(model, storage_id = nil)
        super
        @path               ||= 'backups'
        @cloud_environment  ||= 'AzureCloud'
        path.sub!(%r{^/}, "")

        check_configuration
      end

      def azure_storage_service
        @azure_storage_service ||= Fog::Storage.new(
          provider: 'AzureRM',
          azure_storage_account_name: storage_account,
          azure_storage_access_key: storage_access_key,
          environment: cloud_environment
        )
      end

      def container
        @container ||= azure_storage_service.directories.get(container_name, max_keys: 1)
      end

      def transfer!
        package.filenames.each do |filename|
          source_path = File.join(Config.tmp_path, filename)
          destination_path = File.join(remote_path, filename)
          Logger.info "Storage::Azure uploading '#{container.key}/#{destination_path}'"

          File.open(source_path, 'rb') do |file|
            options = {
              key: destination_path,
              body: file
            }

            block_blob = container.files.create(options)
          end

          Logger.info "Storage::Azure uploaded '#{container.key}/#{block_blob.key}'"
        end
      end

      # Called by the Cycler.
      # Any error raised will be logged as a warning.
      def remove!(package)
        Logger.info "Removing backup package dated #{package.time}..."

        package.filenames.each do |filename|
          destination_path = "#{remote_path_for(package)}/#{filename}"
          Logger.info "Storage::Azure deleting '#{destination_path}'"
          container.files.head(destination_path).destroy
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
