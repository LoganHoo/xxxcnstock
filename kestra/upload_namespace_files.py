#!/usr/bin/env python3
"""
上传项目文件到 Kestra Namespace Files

通过 Kestra API 将本地脚本上传到命名空间存储，
使工作流可以在容器内访问这些文件。
"""

import os
import sys
import argparse
from pathlib import Path
from typing import List, Tuple

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from kestra.lib.kestra_client import KestraClient


class NamespaceFileUploader:
    """Namespace Files 上传器"""
    
    def __init__(self, namespace: str = "xcnstock"):
        self.client = KestraClient()
        self.namespace = namespace
        self.project_root = Path("/Volumes/Xdata/workstation/xxxcnstock")
        
    def upload_file(self, local_path: Path, remote_path: str = None) -> Tuple[bool, str]:
        """
        上传单个文件到 Namespace Files
        
        Args:
            local_path: 本地文件路径
            remote_path: 远程路径（相对于命名空间根目录）
            
        Returns:
            (成功标志, 消息)
        """
        if not local_path.exists():
            return False, f"文件不存在: {local_path}"
            
        if remote_path is None:
            # 自动计算相对路径
            try:
                remote_path = str(local_path.relative_to(self.project_root))
            except ValueError:
                remote_path = local_path.name
                
        # 确保路径以 / 开头
        if not remote_path.startswith('/'):
            remote_path = '/' + remote_path
            
        try:
            with open(local_path, 'rb') as f:
                content = f.read()
                
            # 使用 API 上传文件
            url = f"{self.client.api_url}/namespaces/{self.namespace}/files"
            
            # 使用 PUT 方法上传
            response = self.client._session.put(
                url,
                params={'path': remote_path},
                data=content,
                headers={'Content-Type': 'application/octet-stream'}
            )
            
            if response.status_code in [200, 201, 204]:
                return True, f"✅ 上传成功: {remote_path}"
            else:
                return False, f"❌ 上传失败: {response.status_code} - {response.text[:200]}"
                
        except Exception as e:
            return False, f"❌ 上传异常: {str(e)}"
            
    def upload_directory(self, local_dir: Path, remote_prefix: str = "") -> List[Tuple[bool, str]]:
        """
        上传整个目录到 Namespace Files
        
        Args:
            local_dir: 本地目录路径
            remote_prefix: 远程路径前缀
            
        Returns:
            上传结果列表
        """
        results = []
        
        if not local_dir.exists():
            return [(False, f"目录不存在: {local_dir}")]
            
        # 获取所有 Python 文件
        python_files = list(local_dir.rglob("*.py"))
        
        print(f"📁 准备上传 {len(python_files)} 个文件从 {local_dir}")
        print("=" * 60)
        
        for i, file_path in enumerate(python_files, 1):
            try:
                rel_path = file_path.relative_to(self.project_root)
                remote_path = f"/{remote_prefix}/{rel_path}" if remote_prefix else f"/{rel_path}"
                
                success, message = self.upload_file(file_path, remote_path)
                results.append((success, message))
                
                status = "✅" if success else "❌"
                print(f"  [{i}/{len(python_files)}] {status} {rel_path}")
                
            except Exception as e:
                results.append((False, f"❌ {file_path}: {str(e)}"))
                print(f"  [{i}/{len(python_files)}] ❌ {file_path.name}: {str(e)}")
                
        return results
        
    def list_files(self, path: str = "/") -> Tuple[bool, List[str]]:
        """
        列出 Namespace Files
        
        Args:
            path: 远程路径
            
        Returns:
            (成功标志, 文件列表)
        """
        try:
            url = f"{self.client.api_url}/namespaces/{self.namespace}/files"
            response = self.client._session.get(
                url,
                params={'path': path}
            )
            
            if response.status_code == 200:
                files = response.json()
                return True, files
            else:
                return False, [f"错误: {response.status_code}"]
                
        except Exception as e:
            return False, [f"异常: {str(e)}"]


def main():
    parser = argparse.ArgumentParser(description='上传文件到 Kestra Namespace Files')
    parser.add_argument('--scripts', action='store_true', help='上传 scripts 目录')
    parser.add_argument('--file', type=str, help='上传单个文件')
    parser.add_argument('--list', action='store_true', help='列出已上传的文件')
    parser.add_argument('--namespace', type=str, default='xcnstock', help='命名空间')
    
    args = parser.parse_args()
    
    uploader = NamespaceFileUploader(namespace=args.namespace)
    
    if args.list:
        print(f"📂 {args.namespace} 命名空间文件列表:")
        print("=" * 60)
        success, files = uploader.list_files()
        if success:
            for f in files:
                print(f"  📄 {f}")
        else:
            print(f"  ❌ 获取失败: {files[0]}")
            
    elif args.file:
        file_path = Path(args.file)
        success, message = uploader.upload_file(file_path)
        print(message)
        
    elif args.scripts:
        scripts_dir = Path("/Volumes/Xdata/workstation/xxxcnstock/scripts")
        results = uploader.upload_directory(scripts_dir)
        
        success_count = sum(1 for s, _ in results if s)
        print("\n" + "=" * 60)
        print(f"上传完成: {success_count}/{len(results)} 成功")
        
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
